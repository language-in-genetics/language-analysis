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

	_ "github.com/lib/pq"
)

type manifest struct {
	Version                int       `json:"version"`
	CreatedAt              time.Time `json:"created_at"`
	OutputPath             string    `json:"output_path"`
	Records                int       `json:"records"`
	DuplicateHash          int       `json:"duplicate_hashes"`
	MissingTextFingerprint int       `json:"missing_text_fingerprints"`
	Elapsed                string    `json:"elapsed"`
	Query                  string    `json:"query"`
}

func main() {
	var dbConn string
	var outPath string
	var manifestPath string
	var reportEvery int
	var expectedRecords int
	var limit int
	flag.StringVar(&dbConn, "dbconn", "", "PostgreSQL connection string")
	flag.StringVar(&outPath, "out", "", "Output compact DOI cache path")
	flag.StringVar(&manifestPath, "manifest", "", "Output manifest JSON path")
	flag.IntVar(&reportEvery, "report-every", 1_000_000, "Log progress every N rows")
	flag.IntVar(&expectedRecords, "expected-records", 0, "Optional capacity hint for record count")
	flag.IntVar(&limit, "limit", 0, "Optional row limit for smoke tests")
	flag.Parse()

	if outPath == "" {
		log.Fatal("-out is required")
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

	query := `
SELECT
    v.work_id,
    v.id,
    w.normalized_doi,
    v.text_fingerprint
FROM public.crossref_work_versions v
JOIN public.crossref_works w
  ON w.id = v.work_id
WHERE v.is_current
  AND w.normalized_doi IS NOT NULL;
`
	if limit > 0 {
		query = strings.TrimSuffix(query, ";\n") + "\nLIMIT " + strconv.Itoa(limit) + ";\n"
	}
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("error querying current DOI corpus: %v", err)
	}
	defer rows.Close()

	records := make([]crossrefcache.Record, 0, expectedRecords)
	missingFingerprint := 0
	for rows.Next() {
		var workID int64
		var versionID int64
		var doi string
		var storedFingerprint sql.NullString
		if err := rows.Scan(&workID, &versionID, &doi, &storedFingerprint); err != nil {
			log.Fatalf("error scanning current DOI row: %v", err)
		}

		record := crossrefcache.Record{
			DOIHash:       crossrefcache.HashDOI(doi),
			WorkID:        uint64(workID),
			WorkVersionID: uint64(versionID),
		}
		if storedFingerprint.Valid && len(storedFingerprint.String) == 64 {
			if decoded, err := hex.DecodeString(storedFingerprint.String); err == nil && len(decoded) == 32 {
				copy(record.TextFingerprint[:], decoded)
			}
		}
		if record.TextFingerprint == ([32]byte{}) {
			missingFingerprint++
		}
		records = append(records, record)
		if reportEvery > 0 && len(records)%reportEvery == 0 {
			log.Printf("loaded %d DOI cache records", len(records))
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("error reading current DOI rows: %v", err)
	}

	log.Printf("sorting %d DOI cache records", len(records))
	crossrefcache.Sort(records)
	duplicates := countDuplicateHashes(records)
	if duplicates != 0 {
		log.Printf("warning: %d duplicate DOI hashes detected", duplicates)
	}

	log.Printf("writing %s", outPath)
	if err := crossrefcache.WriteFile(outPath, records); err != nil {
		log.Fatalf("error writing cache: %v", err)
	}
	if err := writeManifest(manifestPath, manifest{
		Version:                int(crossrefcache.Version),
		CreatedAt:              time.Now(),
		OutputPath:             outPath,
		Records:                len(records),
		DuplicateHash:          duplicates,
		MissingTextFingerprint: missingFingerprint,
		Elapsed:                time.Since(start).Round(time.Second).String(),
		Query:                  query,
	}); err != nil {
		log.Fatalf("error writing manifest: %v", err)
	}
	log.Printf("completed DOI cache build: records=%d duplicates=%d missing_fingerprints=%d elapsed=%s", len(records), duplicates, missingFingerprint, time.Since(start).Round(time.Second))
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
