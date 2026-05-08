package main

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"os"
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

func main() {
	var dbConn string
	var outPath string
	var manifestPath string
	var format string
	var computeMissingFingerprints bool
	var reportEvery int
	var expectedRecords int
	var limit int
	flag.StringVar(&dbConn, "dbconn", "", "PostgreSQL connection string")
	flag.StringVar(&outPath, "out", "", "Output compact DOI cache path")
	flag.StringVar(&manifestPath, "manifest", "", "Output manifest JSON path")
	flag.StringVar(&format, "format", "binary", "Output cache format: binary or sqlite")
	flag.BoolVar(&computeMissingFingerprints, "compute-missing-fingerprints", true, "Compute title/abstract fingerprints for legacy rows where text_fingerprint is NULL")
	flag.IntVar(&reportEvery, "report-every", 1_000_000, "Log progress every N rows")
	flag.IntVar(&expectedRecords, "expected-records", 0, "Optional capacity hint for record count")
	flag.IntVar(&limit, "limit", 0, "Optional row limit for smoke tests")
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
	db, err := sql.Open("postgres", dbConn)
	if err != nil {
		log.Fatalf("error opening database: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("error pinging database: %v", err)
	}

	query := buildQuery(computeMissingFingerprints, limit)
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("error querying current DOI corpus: %v", err)
	}
	defer rows.Close()

	var sqliteWriter *crossrefcache.SQLiteWriter
	if format == "sqlite" {
		sqliteWriter, err = crossrefcache.NewSQLiteWriter(outPath)
		if err != nil {
			log.Fatalf("error creating SQLite cache: %v", err)
		}
		defer sqliteWriter.Abort()
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
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("error reading current DOI rows: %v", err)
	}

	cacheRecords := len(records)
	duplicates := 0
	if format == "sqlite" {
		cacheRecords = sqliteWriter.Records()
		duplicates = sqliteWriter.Duplicates()
		if duplicates != 0 {
			log.Printf("warning: %d duplicate DOI hashes detected", duplicates)
		}
		log.Printf("finalizing SQLite cache %s", outPath)
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
	} else {
		log.Printf("sorting %d DOI cache records", len(records))
		crossrefcache.Sort(records)
		duplicates = countDuplicateHashes(records)
		if duplicates != 0 {
			log.Printf("warning: %d duplicate DOI hashes detected", duplicates)
		}

		log.Printf("writing %s", outPath)
		if err := crossrefcache.WriteFile(outPath, records); err != nil {
			log.Fatalf("error writing cache: %v", err)
		}
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
