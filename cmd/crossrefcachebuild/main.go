package main

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"crossref-parser/internal/crossrefcache"
	"crossref-parser/internal/crossrefjson"

	_ "github.com/lib/pq"
)

type manifest struct {
	Version                   int       `json:"version"`
	Format                    string    `json:"format"`
	CreatedAt                 time.Time `json:"created_at"`
	OutputPath                string    `json:"output_path"`
	RowsRead                  int       `json:"rows_read"`
	Records                   int       `json:"records"`
	DuplicateHash             int       `json:"duplicate_hashes"`
	StoredTextFingerprints    int       `json:"stored_text_fingerprints"`
	ComputedTextFingerprints  int       `json:"computed_text_fingerprints"`
	MissingTextFingerprint    int       `json:"missing_text_fingerprints"`
	ComputeMissingFingerprint bool      `json:"compute_missing_fingerprints"`
	Elapsed                   string    `json:"elapsed"`
	Query                     string    `json:"query"`
}

type debugStatsWriter struct {
	path    string
	outPath string
	every   int
	sync    bool
	file    *os.File
	encoder *json.Encoder
}

type debugSnapshot struct {
	At                       time.Time         `json:"at"`
	Stage                    string            `json:"stage"`
	RowsRead                 int               `json:"rows_read"`
	StoredTextFingerprints   int               `json:"stored_text_fingerprints"`
	ComputedTextFingerprints int               `json:"computed_text_fingerprints"`
	MissingTextFingerprints  int               `json:"missing_text_fingerprints"`
	Elapsed                  string            `json:"elapsed"`
	Runtime                  runtimeSnapshot   `json:"runtime"`
	ProcStatus               map[string]string `json:"proc_status,omitempty"`
	ProcSmapsRollup          map[string]string `json:"proc_smaps_rollup,omitempty"`
	ProcIO                   map[string]string `json:"proc_io,omitempty"`
	CachePath                string            `json:"cache_path"`
	CacheSizeBytes           int64             `json:"cache_size_bytes"`
	CacheTempPath            string            `json:"cache_temp_path"`
	CacheTempSizeBytes       int64             `json:"cache_temp_size_bytes"`
}

type runtimeSnapshot struct {
	Alloc         uint64  `json:"alloc"`
	TotalAlloc    uint64  `json:"total_alloc"`
	Sys           uint64  `json:"sys"`
	Lookups       uint64  `json:"lookups"`
	Mallocs       uint64  `json:"mallocs"`
	Frees         uint64  `json:"frees"`
	HeapAlloc     uint64  `json:"heap_alloc"`
	HeapSys       uint64  `json:"heap_sys"`
	HeapIdle      uint64  `json:"heap_idle"`
	HeapInuse     uint64  `json:"heap_inuse"`
	HeapReleased  uint64  `json:"heap_released"`
	HeapObjects   uint64  `json:"heap_objects"`
	StackInuse    uint64  `json:"stack_inuse"`
	StackSys      uint64  `json:"stack_sys"`
	NextGC        uint64  `json:"next_gc"`
	LastGC        uint64  `json:"last_gc"`
	NumGC         uint32  `json:"num_gc"`
	GCCPUFraction float64 `json:"gc_cpu_fraction"`
	Goroutines    int     `json:"goroutines"`
}

func main() {
	var dbConn string
	var outPath string
	var manifestPath string
	var format string
	var computeMissingFingerprints bool
	var reportEvery int
	var expectedRecords int
	var limit int
	var debugStatsPath string
	var debugStatsEvery int
	var debugStatsSync bool
	flag.StringVar(&dbConn, "dbconn", "", "PostgreSQL connection string")
	flag.StringVar(&outPath, "out", "", "Output compact DOI cache path")
	flag.StringVar(&manifestPath, "manifest", "", "Output manifest JSON path")
	flag.StringVar(&format, "format", "binary", "Output cache format: binary or sqlite")
	flag.BoolVar(&computeMissingFingerprints, "compute-missing-fingerprints", true, "Compute title/abstract fingerprints for legacy rows where text_fingerprint is NULL")
	flag.IntVar(&reportEvery, "report-every", 1_000_000, "Log progress every N rows")
	flag.IntVar(&expectedRecords, "expected-records", 0, "Optional capacity hint for record count")
	flag.IntVar(&limit, "limit", 0, "Optional row limit for smoke tests")
	flag.StringVar(&debugStatsPath, "debug-stats", "", "Optional JSONL path for periodic process memory and /proc diagnostics")
	flag.IntVar(&debugStatsEvery, "debug-stats-every", 1_000_000, "Write -debug-stats every N rows")
	flag.BoolVar(&debugStatsSync, "debug-stats-sync", false, "fsync the -debug-stats file after each sample")
	flag.Parse()

	if outPath == "" {
		log.Fatal("-out is required")
	}
	if format != "binary" && format != "sqlite" {
		log.Fatal("-format must be binary or sqlite")
	}
	if manifestPath == "" {
		manifestPath = outPath + ".manifest.json"
	}

	start := time.Now()
	debugStats, err := newDebugStatsWriter(debugStatsPath, outPath, debugStatsEvery, debugStatsSync)
	if err != nil {
		log.Fatalf("error opening debug stats: %v", err)
	}
	defer debugStats.Close()
	debugStats.Write("start", start, 0, 0, 0, 0)

	db, err := sql.Open("postgres", dbConn)
	if err != nil {
		log.Fatalf("error opening database: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("error pinging database: %v", err)
	}
	debugStats.Write("database-ready", start, 0, 0, 0, 0)

	query := buildQuery(computeMissingFingerprints, limit)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("error querying current DOI corpus: %v", err)
	}
	defer rows.Close()
	debugStats.Write("query-open", start, 0, 0, 0, 0)

	var sqliteWriter *crossrefcache.SQLiteWriter
	if format == "sqlite" {
		sqliteWriter, err = crossrefcache.NewSQLiteWriter(outPath)
		if err != nil {
			log.Fatalf("error creating SQLite cache: %v", err)
		}
		defer sqliteWriter.Abort()
		debugStats.Write("sqlite-writer-open", start, 0, 0, 0, 0)
	}

	records := make([]crossrefcache.Record, 0, expectedRecords)
	rowsRead := 0
	storedFingerprints := 0
	computedFingerprints := 0
	missingFingerprints := 0
	for rows.Next() {
		record, source, err := scanRecord(rows, computeMissingFingerprints)
		if err != nil {
			log.Fatalf("error scanning current DOI row: %v", err)
		}
		rowsRead++
		switch source {
		case "stored":
			storedFingerprints++
		case "computed":
			computedFingerprints++
		default:
			missingFingerprints++
		}
		if format == "sqlite" {
			if err := sqliteWriter.Insert(record); err != nil {
				log.Fatalf("error writing SQLite cache row: %v", err)
			}
		} else {
			records = append(records, record)
		}
		if reportEvery > 0 && rowsRead%reportEvery == 0 {
			log.Printf("loaded %d DOI cache records", rowsRead)
		}
		if debugStats.ShouldWrite(rowsRead) {
			debugStats.Write("progress", start, rowsRead, storedFingerprints, computedFingerprints, missingFingerprints)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("error reading current DOI rows: %v", err)
	}
	debugStats.Write("rows-complete", start, rowsRead, storedFingerprints, computedFingerprints, missingFingerprints)

	cacheRecords := len(records)
	duplicates := 0
	if format == "sqlite" {
		cacheRecords = sqliteWriter.Records()
		duplicates = sqliteWriter.Duplicates()
		if duplicates != 0 {
			log.Printf("warning: %d duplicate DOI hashes detected", duplicates)
		}
		log.Printf("finalizing SQLite cache %s", outPath)
		debugStats.Write("before-sqlite-finalize", start, rowsRead, storedFingerprints, computedFingerprints, missingFingerprints)
		if err := sqliteWriter.Close(map[string]string{
			"version":                      strconv.Itoa(int(crossrefcache.Version)),
			"format":                       "sqlite",
			"created_at":                   time.Now().Format(time.RFC3339Nano),
			"rows_read":                    strconv.Itoa(rowsRead),
			"stored_text_fingerprints":     strconv.Itoa(storedFingerprints),
			"computed_text_fingerprints":   strconv.Itoa(computedFingerprints),
			"missing_text_fingerprints":    strconv.Itoa(missingFingerprints),
			"compute_missing_fingerprints": strconv.FormatBool(computeMissingFingerprints),
		}); err != nil {
			log.Fatalf("error finalizing SQLite cache: %v", err)
		}
		sqliteWriter = nil
		debugStats.Write("after-sqlite-finalize", start, rowsRead, storedFingerprints, computedFingerprints, missingFingerprints)
	} else {
		log.Printf("sorting %d DOI cache records", len(records))
		debugStats.Write("before-sort", start, rowsRead, storedFingerprints, computedFingerprints, missingFingerprints)
		crossrefcache.Sort(records)
		duplicates = countDuplicateHashes(records)
		if duplicates != 0 {
			log.Printf("warning: %d duplicate DOI hashes detected", duplicates)
		}

		log.Printf("writing %s", outPath)
		debugStats.Write("before-binary-write", start, rowsRead, storedFingerprints, computedFingerprints, missingFingerprints)
		if err := crossrefcache.WriteFile(outPath, records); err != nil {
			log.Fatalf("error writing cache: %v", err)
		}
		debugStats.Write("after-binary-write", start, rowsRead, storedFingerprints, computedFingerprints, missingFingerprints)
	}
	if err := writeManifest(manifestPath, manifest{
		Version:                   int(crossrefcache.Version),
		Format:                    format,
		CreatedAt:                 time.Now(),
		OutputPath:                outPath,
		RowsRead:                  rowsRead,
		Records:                   cacheRecords,
		DuplicateHash:             duplicates,
		StoredTextFingerprints:    storedFingerprints,
		ComputedTextFingerprints:  computedFingerprints,
		MissingTextFingerprint:    missingFingerprints,
		ComputeMissingFingerprint: computeMissingFingerprints,
		Elapsed:                   time.Since(start).Round(time.Second).String(),
		Query:                     query,
	}); err != nil {
		log.Fatalf("error writing manifest: %v", err)
	}
	debugStats.Write("complete", start, rowsRead, storedFingerprints, computedFingerprints, missingFingerprints)
	log.Printf(
		"completed DOI cache build: format=%s rows_read=%d records=%d duplicates=%d stored_fingerprints=%d computed_fingerprints=%d missing_fingerprints=%d elapsed=%s",
		format,
		rowsRead,
		cacheRecords,
		duplicates,
		storedFingerprints,
		computedFingerprints,
		missingFingerprints,
		time.Since(start).Round(time.Second),
	)
}

func newDebugStatsWriter(path string, outPath string, every int, sync bool) (*debugStatsWriter, error) {
	if path == "" {
		return nil, nil
	}
	if every <= 0 {
		every = 1
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &debugStatsWriter{
		path:    path,
		outPath: outPath,
		every:   every,
		sync:    sync,
		file:    file,
		encoder: json.NewEncoder(file),
	}, nil
}

func (w *debugStatsWriter) ShouldWrite(rowsRead int) bool {
	return w != nil && rowsRead > 0 && rowsRead%w.every == 0
}

func (w *debugStatsWriter) Write(stage string, start time.Time, rowsRead int, storedFingerprints int, computedFingerprints int, missingFingerprints int) {
	if w == nil {
		return
	}
	snapshot := debugSnapshot{
		At:                       time.Now(),
		Stage:                    stage,
		RowsRead:                 rowsRead,
		StoredTextFingerprints:   storedFingerprints,
		ComputedTextFingerprints: computedFingerprints,
		MissingTextFingerprints:  missingFingerprints,
		Elapsed:                  time.Since(start).Round(time.Millisecond).String(),
		Runtime:                  readRuntimeSnapshot(),
		ProcStatus:               readProcKeyValues("/proc/self/status"),
		ProcSmapsRollup:          readProcKeyValues("/proc/self/smaps_rollup"),
		ProcIO:                   readProcKeyValues("/proc/self/io"),
		CachePath:                w.outPath,
		CacheSizeBytes:           fileSize(w.outPath),
		CacheTempPath:            w.outPath + ".tmp",
		CacheTempSizeBytes:       fileSize(w.outPath + ".tmp"),
	}
	if err := w.encoder.Encode(snapshot); err != nil {
		log.Printf("warning: could not write debug stats to %s: %v", w.path, err)
		return
	}
	if w.sync {
		if err := w.file.Sync(); err != nil {
			log.Printf("warning: could not sync debug stats %s: %v", w.path, err)
		}
	}
}

func (w *debugStatsWriter) Close() {
	if w == nil || w.file == nil {
		return
	}
	if w.sync {
		if err := w.file.Sync(); err != nil {
			log.Printf("warning: could not sync debug stats %s: %v", w.path, err)
		}
	}
	if err := w.file.Close(); err != nil {
		log.Printf("warning: could not close debug stats %s: %v", w.path, err)
	}
}

func readRuntimeSnapshot() runtimeSnapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return runtimeSnapshot{
		Alloc:         stats.Alloc,
		TotalAlloc:    stats.TotalAlloc,
		Sys:           stats.Sys,
		Lookups:       stats.Lookups,
		Mallocs:       stats.Mallocs,
		Frees:         stats.Frees,
		HeapAlloc:     stats.HeapAlloc,
		HeapSys:       stats.HeapSys,
		HeapIdle:      stats.HeapIdle,
		HeapInuse:     stats.HeapInuse,
		HeapReleased:  stats.HeapReleased,
		HeapObjects:   stats.HeapObjects,
		StackInuse:    stats.StackInuse,
		StackSys:      stats.StackSys,
		NextGC:        stats.NextGC,
		LastGC:        stats.LastGC,
		NumGC:         stats.NumGC,
		GCCPUFraction: stats.GCCPUFraction,
		Goroutines:    runtime.NumGoroutine(),
	}
}

func readProcKeyValues(path string) map[string]string {
	data, err := os.ReadFile(path)
	if err != nil {
		return map[string]string{"error": err.Error()}
	}
	values := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		values[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return values
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return info.Size()
}

func buildQuery(computeMissingFingerprints bool, limit int) string {
	fields := `
SELECT
    v.work_id,
    v.id,
    w.normalized_doi,
    v.text_fingerprint`
	if computeMissingFingerprints {
		fields += `,
    v.title,
    v.abstract`
	}
	// The LIMIT inside the lateral lookup keeps PostgreSQL from flattening this
	// into a large hash join over both Crossref tables. The annual prefilter must
	// stream current versions and use the id->DOI covering index for each row.
	query := fields + `
FROM public.crossref_work_versions v
JOIN LATERAL (
    SELECT w.normalized_doi
    FROM public.crossref_works w
    WHERE w.id = v.work_id
      AND w.normalized_doi IS NOT NULL
    LIMIT 1
) w ON true
WHERE v.is_current;
`
	if limit > 0 {
		query = strings.TrimSuffix(query, ";\n") + "\nLIMIT " + strconv.Itoa(limit) + ";\n"
	}
	return query
}

func scanRecord(rows *sql.Rows, computeMissingFingerprints bool) (crossrefcache.Record, string, error) {
	var workID int64
	var versionID int64
	var doi string
	var storedFingerprint sql.NullString
	var title sql.NullString
	var abstract sql.NullString
	if computeMissingFingerprints {
		if err := rows.Scan(&workID, &versionID, &doi, &storedFingerprint, &title, &abstract); err != nil {
			return crossrefcache.Record{}, "", err
		}
	} else {
		if err := rows.Scan(&workID, &versionID, &doi, &storedFingerprint); err != nil {
			return crossrefcache.Record{}, "", err
		}
	}

	record := crossrefcache.Record{
		DOIHash:       crossrefcache.HashDOI(doi),
		WorkID:        uint64(workID),
		WorkVersionID: uint64(versionID),
	}
	if storedFingerprint.Valid && len(storedFingerprint.String) == 64 {
		if decoded, err := hex.DecodeString(storedFingerprint.String); err == nil && len(decoded) == 32 {
			copy(record.TextFingerprint[:], decoded)
			return record, "stored", nil
		}
	}
	if computeMissingFingerprints {
		record.TextFingerprint = crossrefjson.FingerprintFromText(title.String, abstract.String)
		return record, "computed", nil
	}
	return record, "missing", nil
}

func countDuplicateHashes(records []crossrefcache.Record) int {
	duplicates := 0
	for i := 1; i < len(records); i++ {
		if records[i-1].DOIHash == records[i].DOIHash {
			duplicates++
		}
	}
	return duplicates
}

func writeManifest(path string, value manifest) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}
