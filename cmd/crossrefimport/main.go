package main

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lib/pq"
)

const defaultConn = ""

type importOptions struct {
	dir          string
	fromRawText  bool
	runLabel     string
	sourceType   string
	sourcePath   string
	importedBy   string
	dbConn       string
	batchSize    int
	limit        int
	maxLineBytes int
}

type importStats struct {
	seen           int
	staged         int
	rejected       int
	badJSON        int
	missingDOI     int
	versionInserts int
	batches        int
}

type stagedRecord struct {
	sourceRef     string
	rawJSON       string
	originalDOI   string
	normalizedDOI string
	payloadSHA256 string
	title         *string
	abstract      *string
	journalName   *string
	pubYear       *int
	recordType    *string
}

type batchProcessor struct {
	db              *sql.DB
	runID           int64
	enableLegacyMap bool
	batchSize       int
	records         []stagedRecord
	stats           *importStats
}

func main() {
	opts := parseFlags()

	db, err := sql.Open("postgres", opts.dbConn)
	if err != nil {
		log.Fatalf("error connecting to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("error pinging database: %v", err)
	}

	if err := ensureSchema(db); err != nil {
		log.Fatalf("error ensuring schema: %v", err)
	}

	runID, err := createImportRun(db, opts)
	if err != nil {
		log.Fatalf("error creating import run: %v", err)
	}

	stats := &importStats{}
	success := false
	defer func() {
		status := "failed"
		if success {
			status = "completed"
		}
		if err := finalizeImportRun(db, runID, status, *stats); err != nil {
			log.Printf("warning: could not finalize import run %d: %v", runID, err)
		}
	}()

	processor := &batchProcessor{
		db:              db,
		runID:           runID,
		enableLegacyMap: opts.fromRawText,
		batchSize:       opts.batchSize,
		stats:           stats,
	}

	if opts.fromRawText {
		err = importFromRawText(db, opts, processor)
	} else {
		err = importFromDirectory(opts, processor)
	}
	if err != nil {
		log.Fatalf("import failed: %v", err)
	}

	if err := processor.flush(); err != nil {
		log.Fatalf("final flush failed: %v", err)
	}

	success = true
	log.Printf(
		"completed import run %d: seen=%d staged=%d rejected=%d version_inserts=%d batches=%d",
		runID,
		stats.seen,
		stats.staged,
		stats.rejected,
		stats.versionInserts,
		stats.batches,
	)
}

func parseFlags() importOptions {
	opts := importOptions{}
	flag.StringVar(&opts.dir, "dir", "", "Directory containing Crossref *.jsonl.gz files")
	flag.BoolVar(&opts.fromRawText, "from-raw-text", false, "Backfill from public.raw_text_data instead of reading files")
	flag.StringVar(&opts.runLabel, "run-label", "", "Unique label for this import run")
	flag.StringVar(&opts.sourceType, "source-type", "", "Source type recorded in import_runs")
	flag.StringVar(&opts.sourcePath, "source-path", "", "Source path recorded in import_runs")
	flag.StringVar(&opts.importedBy, "imported-by", defaultImportedBy(), "User recorded in import_runs")
	flag.StringVar(&opts.dbConn, "dbconn", defaultConn, "PostgreSQL connection string")
	flag.IntVar(&opts.batchSize, "batch-size", 10000, "Rows per staging batch")
	flag.IntVar(&opts.limit, "limit", 0, "Stop after importing this many records")
	flag.IntVar(&opts.maxLineBytes, "max-line-bytes", 64*1024*1024, "Maximum JSON line size when reading snapshot files")
	flag.Parse()

	if opts.runLabel == "" {
		log.Fatal("-run-label is required")
	}
	if opts.batchSize <= 0 {
		log.Fatal("-batch-size must be positive")
	}
	if opts.maxLineBytes <= 0 {
		log.Fatal("-max-line-bytes must be positive")
	}
	if opts.fromRawText == (opts.dir != "") {
		log.Fatal("choose exactly one source: either -dir or -from-raw-text")
	}

	if opts.sourceType == "" {
		if opts.fromRawText {
			opts.sourceType = "legacy_raw_text"
		} else {
			opts.sourceType = "annual_dump"
		}
	}

	if opts.sourcePath == "" {
		if opts.fromRawText {
			opts.sourcePath = "public.raw_text_data"
		} else {
			absDir, err := filepath.Abs(opts.dir)
			if err != nil {
				log.Fatalf("could not resolve absolute path for %q: %v", opts.dir, err)
			}
			opts.sourcePath = absDir
		}
	}

	return opts
}

func defaultImportedBy() string {
	if current, err := user.Current(); err == nil && current.Username != "" {
		return current.Username
	}
	if value := os.Getenv("USER"); value != "" {
		return value
	}
	return "unknown"
}

func ensureSchema(db *sql.DB) error {
	schema := `
CREATE SCHEMA IF NOT EXISTS languageingenetics;

CREATE TABLE IF NOT EXISTS languageingenetics.import_runs (
    id BIGSERIAL PRIMARY KEY,
    run_label TEXT NOT NULL UNIQUE,
    source_type TEXT NOT NULL,
    source_path TEXT NOT NULL,
    snapshot_date DATE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    imported_by TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS languageingenetics.works (
    id BIGSERIAL PRIMARY KEY,
    normalized_doi TEXT,
    original_doi TEXT,
    first_import_run_id BIGINT NOT NULL REFERENCES languageingenetics.import_runs(id),
    latest_import_run_id BIGINT NOT NULL REFERENCES languageingenetics.import_runs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS works_normalized_doi_idx
    ON languageingenetics.works(normalized_doi)
    WHERE normalized_doi IS NOT NULL;

CREATE TABLE IF NOT EXISTS languageingenetics.work_versions (
    id BIGSERIAL PRIMARY KEY,
    work_id BIGINT NOT NULL REFERENCES languageingenetics.works(id) ON DELETE CASCADE,
    import_run_id BIGINT NOT NULL REFERENCES languageingenetics.import_runs(id),
    raw_json_text TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    title TEXT,
    abstract TEXT,
    journal_name TEXT,
    pub_year INT,
    record_type TEXT,
    is_current BOOLEAN NOT NULL DEFAULT false,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (work_id, payload_sha256)
);

CREATE UNIQUE INDEX IF NOT EXISTS work_versions_current_idx
    ON languageingenetics.work_versions(work_id)
    WHERE is_current;

CREATE INDEX IF NOT EXISTS work_versions_work_payload_idx
    ON languageingenetics.work_versions(work_id, payload_sha256);

CREATE TABLE IF NOT EXISTS languageingenetics.legacy_raw_text_map (
    raw_text_data_id BIGINT PRIMARY KEY,
    work_id BIGINT NOT NULL REFERENCES languageingenetics.works(id),
    work_version_id BIGINT NOT NULL REFERENCES languageingenetics.work_versions(id)
);

CREATE TABLE IF NOT EXISTS languageingenetics.import_rejections (
    id BIGSERIAL PRIMARY KEY,
    import_run_id BIGINT NOT NULL REFERENCES languageingenetics.import_runs(id) ON DELETE CASCADE,
    source_ref TEXT,
    reason TEXT NOT NULL,
    raw_json_text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW languageingenetics.current_works AS
SELECT
    w.id AS work_id,
    v.id AS work_version_id,
    w.normalized_doi,
    w.original_doi,
    v.raw_json_text,
    v.title,
    v.abstract,
    v.journal_name,
    v.pub_year,
    v.record_type
FROM languageingenetics.works w
JOIN languageingenetics.work_versions v
  ON v.work_id = w.id
WHERE v.is_current;
`
	_, err := db.Exec(schema)
	return err
}

func createImportRun(db *sql.DB, opts importOptions) (int64, error) {
	var runID int64
	err := db.QueryRow(
		`INSERT INTO languageingenetics.import_runs (run_label, source_type, source_path, imported_by)
         VALUES ($1, $2, $3, $4)
         RETURNING id`,
		opts.runLabel,
		opts.sourceType,
		opts.sourcePath,
		opts.importedBy,
	).Scan(&runID)
	if err != nil {
		return 0, err
	}
	return runID, nil
}

func finalizeImportRun(db *sql.DB, runID int64, status string, stats importStats) error {
	notes := fmt.Sprintf(
		"seen=%d staged=%d rejected=%d bad_json=%d missing_doi=%d version_inserts=%d batches=%d",
		stats.seen,
		stats.staged,
		stats.rejected,
		stats.badJSON,
		stats.missingDOI,
		stats.versionInserts,
		stats.batches,
	)
	_, err := db.Exec(
		`UPDATE languageingenetics.import_runs
         SET status = $2, completed_at = now(), notes = $3
         WHERE id = $1`,
		runID,
		status,
		notes,
	)
	return err
}

func importFromDirectory(opts importOptions, processor *batchProcessor) error {
	pattern := filepath.Join(opts.dir, "*.jsonl.gz")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("could not glob %q: %w", pattern, err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return fmt.Errorf("no *.jsonl.gz files found under %s", opts.dir)
	}

	for _, filename := range files {
		log.Printf("reading %s", filename)
		if err := importOneGzipFile(filename, opts, processor); err != nil {
			return err
		}
		if opts.limit > 0 && processor.stats.seen >= opts.limit {
			break
		}
	}

	return nil
}

func importOneGzipFile(filename string, opts importOptions, processor *batchProcessor) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening %s: %w", filename, err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("error creating gzip reader for %s: %w", filename, err)
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)
	scanner.Buffer(make([]byte, 1024*1024), opts.maxLineBytes)

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}

		processor.stats.seen++
		record, rejectReason, err := parseRecord(raw, fmt.Sprintf("%s:%d", filepath.Base(filename), lineNumber))
		if err != nil {
			return fmt.Errorf("error parsing %s line %d: %w", filename, lineNumber, err)
		}
		if rejectReason != "" {
			if err := recordRejection(processor.db, processor.runID, record.sourceRef, rejectReason, raw); err != nil {
				return err
			}
			updateRejectStats(processor.stats, rejectReason)
			if limitReached(processor.stats, opts.limit) {
				break
			}
			continue
		}
		if err := processor.add(*record); err != nil {
			return err
		}
		if limitReached(processor.stats, opts.limit) {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning %s: %w", filename, err)
	}
	return nil
}

func importFromRawText(db *sql.DB, opts importOptions, processor *batchProcessor) error {
	rows, err := db.Query(`SELECT id, filesrc FROM public.raw_text_data ORDER BY id`)
	if err != nil {
		return fmt.Errorf("error reading public.raw_text_data: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var rawID int64
		var raw string
		if err := rows.Scan(&rawID, &raw); err != nil {
			return fmt.Errorf("error scanning public.raw_text_data row: %w", err)
		}

		processor.stats.seen++
		record, rejectReason, err := parseRecord(raw, fmt.Sprintf("%d", rawID))
		if err != nil {
			return fmt.Errorf("error parsing raw_text_data id %d: %w", rawID, err)
		}
		if rejectReason != "" {
			if err := recordRejection(processor.db, processor.runID, record.sourceRef, rejectReason, raw); err != nil {
				return err
			}
			updateRejectStats(processor.stats, rejectReason)
			if limitReached(processor.stats, opts.limit) {
				break
			}
			continue
		}
		if err := processor.add(*record); err != nil {
			return err
		}
		if limitReached(processor.stats, opts.limit) {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating public.raw_text_data: %w", err)
	}
	return nil
}

func limitReached(stats *importStats, limit int) bool {
	return limit > 0 && stats.seen >= limit
}

func recordRejection(db *sql.DB, runID int64, sourceRef, reason, rawJSON string) error {
	_, err := db.Exec(
		`INSERT INTO languageingenetics.import_rejections (import_run_id, source_ref, reason, raw_json_text)
         VALUES ($1, $2, $3, $4)`,
		runID,
		sourceRef,
		reason,
		rawJSON,
	)
	return err
}

func updateRejectStats(stats *importStats, reason string) {
	stats.rejected++
	switch reason {
	case "bad_json":
		stats.badJSON++
	case "missing_doi":
		stats.missingDOI++
	}
}

func parseRecord(rawJSON, sourceRef string) (*stagedRecord, string, error) {
	record := &stagedRecord{
		sourceRef: sourceRef,
		rawJSON:   rawJSON,
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(rawJSON), &payload); err != nil {
		return record, "bad_json", nil
	}

	record.originalDOI = strings.TrimSpace(extractString(payload["DOI"]))
	record.normalizedDOI = normalizeDOI(record.originalDOI)
	if record.normalizedDOI == "" {
		return record, "missing_doi", nil
	}

	record.payloadSHA256 = sha256Hex(rawJSON)
	record.title = optionalString(firstString(payload["title"]))
	record.abstract = optionalString(extractString(payload["abstract"]))
	record.journalName = optionalString(firstString(payload["container-title"]))
	record.pubYear = extractPubYear(payload)
	record.recordType = optionalString(extractString(payload["type"]))

	return record, "", nil
}

func normalizeDOI(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func extractString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case []any:
		return firstString(typed)
	default:
		return ""
	}
}

func firstString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case []any:
		for _, item := range typed {
			if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
				return strings.TrimSpace(text)
			}
		}
	}
	return ""
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	copyValue := value
	return &copyValue
}

func extractPubYear(payload map[string]any) *int {
	for _, key := range []string{"published", "issued", "published-print", "published-online", "created"} {
		if year, ok := extractYearFromDateParts(payload[key]); ok {
			return &year
		}
	}
	return nil
}

func extractYearFromDateParts(value any) (int, bool) {
	container, ok := value.(map[string]any)
	if !ok {
		return 0, false
	}

	dateParts, ok := container["date-parts"].([]any)
	if !ok || len(dateParts) == 0 {
		return 0, false
	}
	firstRow, ok := dateParts[0].([]any)
	if !ok || len(firstRow) == 0 {
		return 0, false
	}

	switch year := firstRow[0].(type) {
	case float64:
		return int(year), true
	case int:
		return year, true
	default:
		return 0, false
	}
}

func (p *batchProcessor) add(record stagedRecord) error {
	p.records = append(p.records, record)
	p.stats.staged++
	if len(p.records) >= p.batchSize {
		return p.flush()
	}
	return nil
}

func (p *batchProcessor) flush() error {
	if len(p.records) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`
CREATE TEMP TABLE import_stage (
    source_ref TEXT NOT NULL,
    raw_json_text TEXT NOT NULL,
    original_doi TEXT NOT NULL,
    normalized_doi TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    title TEXT,
    abstract TEXT,
    journal_name TEXT,
    pub_year INT,
    record_type TEXT
) ON COMMIT DROP;
`); err != nil {
		return fmt.Errorf("error creating import_stage: %w", err)
	}

	stmt, err := tx.Prepare(pq.CopyIn(
		"import_stage",
		"source_ref",
		"raw_json_text",
		"original_doi",
		"normalized_doi",
		"payload_sha256",
		"title",
		"abstract",
		"journal_name",
		"pub_year",
		"record_type",
	))
	if err != nil {
		return fmt.Errorf("error preparing COPY INTO import_stage: %w", err)
	}

	for _, record := range p.records {
		if _, err := stmt.Exec(
			record.sourceRef,
			record.rawJSON,
			record.originalDOI,
			record.normalizedDOI,
			record.payloadSHA256,
			nilString(record.title),
			nilString(record.abstract),
			nilString(record.journalName),
			nilInt(record.pubYear),
			nilString(record.recordType),
		); err != nil {
			stmt.Close()
			return fmt.Errorf("error copying staged row: %w", err)
		}
	}

	if _, err := stmt.Exec(); err != nil {
		stmt.Close()
		return fmt.Errorf("error finalizing COPY INTO import_stage: %w", err)
	}
	if err := stmt.Close(); err != nil {
		return fmt.Errorf("error closing COPY INTO import_stage: %w", err)
	}

	if _, err := tx.Exec(`
WITH dedup AS (
    SELECT DISTINCT normalized_doi, original_doi
    FROM import_stage
)
INSERT INTO languageingenetics.works (
    normalized_doi,
    original_doi,
    first_import_run_id,
    latest_import_run_id
)
SELECT
    d.normalized_doi,
    d.original_doi,
    $1,
    $1
FROM dedup d
ON CONFLICT (normalized_doi) WHERE normalized_doi IS NOT NULL
DO UPDATE SET
    original_doi = EXCLUDED.original_doi,
    latest_import_run_id = EXCLUDED.latest_import_run_id,
    updated_at = now();
`, p.runID); err != nil {
		return fmt.Errorf("error upserting works: %w", err)
	}

	if _, err := tx.Exec(`
CREATE TEMP TABLE stage_resolved ON COMMIT DROP AS
SELECT
    s.source_ref,
    s.raw_json_text,
    s.payload_sha256,
    s.title,
    s.abstract,
    s.journal_name,
    s.pub_year,
    s.record_type,
    w.id AS work_id,
    current_v.id AS current_version_id,
    current_v.payload_sha256 AS current_payload_sha256,
    existing_v.id AS existing_version_id
FROM import_stage s
JOIN languageingenetics.works w
  ON w.normalized_doi = s.normalized_doi
LEFT JOIN languageingenetics.work_versions current_v
  ON current_v.work_id = w.id
 AND current_v.is_current
LEFT JOIN languageingenetics.work_versions existing_v
  ON existing_v.work_id = w.id
 AND existing_v.payload_sha256 = s.payload_sha256;
`); err != nil {
		return fmt.Errorf("error creating stage_resolved: %w", err)
	}

	result, err := tx.Exec(`
INSERT INTO languageingenetics.work_versions (
    work_id,
    import_run_id,
    raw_json_text,
    payload_sha256,
    title,
    abstract,
    journal_name,
    pub_year,
    record_type,
    is_current
)
SELECT DISTINCT ON (work_id, payload_sha256)
    work_id,
    $1,
    raw_json_text,
    payload_sha256,
    title,
    abstract,
    journal_name,
    pub_year,
    record_type,
    false
FROM stage_resolved
WHERE existing_version_id IS NULL
ORDER BY work_id, payload_sha256;
`, p.runID)
	if err != nil {
		return fmt.Errorf("error inserting work_versions: %w", err)
	}
	if rowsAffected, err := result.RowsAffected(); err == nil {
		p.stats.versionInserts += int(rowsAffected)
	}

	if _, err := tx.Exec(`
CREATE TEMP TABLE stage_changes ON COMMIT DROP AS
SELECT DISTINCT
    work_id,
    payload_sha256
FROM stage_resolved
WHERE current_version_id IS NULL
   OR current_payload_sha256 IS DISTINCT FROM payload_sha256;
`); err != nil {
		return fmt.Errorf("error creating stage_changes: %w", err)
	}

	if _, err := tx.Exec(`
UPDATE languageingenetics.work_versions
SET is_current = false
WHERE is_current
  AND work_id IN (SELECT work_id FROM stage_changes);
`); err != nil {
		return fmt.Errorf("error clearing current flags: %w", err)
	}

	if _, err := tx.Exec(`
UPDATE languageingenetics.work_versions v
SET is_current = true
FROM stage_changes c
WHERE v.work_id = c.work_id
  AND v.payload_sha256 = c.payload_sha256;
`); err != nil {
		return fmt.Errorf("error setting current flags: %w", err)
	}

	if p.enableLegacyMap {
		if _, err := tx.Exec(`
INSERT INTO languageingenetics.legacy_raw_text_map (
    raw_text_data_id,
    work_id,
    work_version_id
)
SELECT DISTINCT ON (sr.source_ref)
    CAST(sr.source_ref AS BIGINT),
    sr.work_id,
    v.id
FROM stage_resolved sr
JOIN languageingenetics.work_versions v
  ON v.work_id = sr.work_id
 AND v.payload_sha256 = sr.payload_sha256
WHERE sr.source_ref ~ '^[0-9]+$'
ORDER BY sr.source_ref
ON CONFLICT (raw_text_data_id)
DO UPDATE SET
    work_id = EXCLUDED.work_id,
    work_version_id = EXCLUDED.work_version_id;
`); err != nil {
			return fmt.Errorf("error updating legacy_raw_text_map: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing staged batch: %w", err)
	}

	p.stats.batches++
	log.Printf(
		"flushed batch %d: rows=%d total_staged=%d total_rejected=%d total_version_inserts=%d",
		p.stats.batches,
		len(p.records),
		p.stats.staged,
		p.stats.rejected,
		p.stats.versionInserts,
	)
	p.records = p.records[:0]
	return nil
}

func nilString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nilInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}
