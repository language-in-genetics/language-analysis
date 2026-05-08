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
	"html"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
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
	seen            int
	staged          int
	rejected        int
	badJSON         int
	missingDOI      int
	missingIdentity int
	versionInserts  int
	batches         int
}

type stagedRecord struct {
	sourceRef        string
	rawJSON          string
	originalDOI      string
	normalizedDOI    string
	payloadSHA256    string
	title            *string
	abstract         *string
	journalName      *string
	pubYear          *int
	pubDate          *string
	recordType       *string
	titleNorm        string
	abstractNorm     string
	textFingerprint  string
	fallbackIdentity string
}

type batchProcessor struct {
	db                         *sql.DB
	runID                      int64
	enableLegacyMap            bool
	batchSize                  int
	records                    []stagedRecord
	stats                      *importStats
	previousMaxPublicationDate sql.NullString
	previousMaxPublicationYear sql.NullInt64
}

var (
	htmlTagPattern      = regexp.MustCompile(`<[^>]+>`)
	whitespacePattern   = regexp.MustCompile(`\s+`)
	unicodeDashReplacer = strings.NewReplacer(
		"\u2010", "-",
		"\u2011", "-",
		"\u2012", "-",
		"\u2013", "-",
		"\u2014", "-",
		"\u2212", "-",
	)
)

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

	previousMaxPublicationDate, previousMaxPublicationYear, err := previousPublicationCutoff(db, runID)
	if err != nil {
		log.Fatalf("error reading previous publication cutoff: %v", err)
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
		db:                         db,
		runID:                      runID,
		enableLegacyMap:            opts.fromRawText,
		batchSize:                  opts.batchSize,
		stats:                      stats,
		previousMaxPublicationDate: previousMaxPublicationDate,
		previousMaxPublicationYear: previousMaxPublicationYear,
	}

	if opts.fromRawText {
		err = importFromRawText(db, opts, processor)
	} else {
		err = importFromDirectory(opts, processor)
	}
	if err != nil {
		log.Printf("import failed: %v", err)
		return
	}

	if err := processor.flush(); err != nil {
		log.Printf("final flush failed: %v", err)
		return
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
CREATE TABLE IF NOT EXISTS public.crossref_import_runs (
    id BIGSERIAL PRIMARY KEY,
    run_label TEXT NOT NULL UNIQUE,
    source_type TEXT NOT NULL,
    source_path TEXT NOT NULL,
    snapshot_date DATE,
    max_publication_date DATE,
    max_publication_year INT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    imported_by TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS public.crossref_works (
    id BIGSERIAL PRIMARY KEY,
    normalized_doi TEXT,
    original_doi TEXT,
    fallback_identity TEXT,
    first_import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    latest_import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS crossref_works_normalized_doi_idx
    ON public.crossref_works(normalized_doi)
    WHERE normalized_doi IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.crossref_work_versions (
    id BIGSERIAL PRIMARY KEY,
    work_id BIGINT NOT NULL REFERENCES public.crossref_works(id) ON DELETE CASCADE,
    import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    raw_json_text TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    title TEXT,
    abstract TEXT,
    journal_name TEXT,
    pub_year INT,
    pub_date DATE,
    record_type TEXT,
    title_norm TEXT,
    abstract_norm TEXT,
    text_fingerprint TEXT,
    is_current BOOLEAN NOT NULL DEFAULT false,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (work_id, payload_sha256)
);

CREATE UNIQUE INDEX IF NOT EXISTS crossref_work_versions_current_idx
    ON public.crossref_work_versions(work_id)
    WHERE is_current;

CREATE TABLE IF NOT EXISTS public.crossref_work_text_changes (
    id BIGSERIAL PRIMARY KEY,
    work_id BIGINT NOT NULL REFERENCES public.crossref_works(id) ON DELETE CASCADE,
    from_work_version_id BIGINT REFERENCES public.crossref_work_versions(id) ON DELETE SET NULL,
    to_work_version_id BIGINT NOT NULL REFERENCES public.crossref_work_versions(id) ON DELETE CASCADE,
    from_import_run_id BIGINT REFERENCES public.crossref_import_runs(id),
    to_import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    previous_title TEXT,
    previous_abstract TEXT,
    new_title TEXT,
    new_abstract TEXT,
    previous_title_norm TEXT,
    previous_abstract_norm TEXT,
    new_title_norm TEXT,
    new_abstract_norm TEXT,
    previous_text_fingerprint TEXT,
    new_text_fingerprint TEXT,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (work_id, from_work_version_id, to_work_version_id)
);

CREATE TABLE IF NOT EXISTS public.crossref_legacy_raw_text_map (
    raw_text_data_id BIGINT PRIMARY KEY,
    work_id BIGINT NOT NULL REFERENCES public.crossref_works(id),
    work_version_id BIGINT NOT NULL REFERENCES public.crossref_work_versions(id)
);

CREATE TABLE IF NOT EXISTS public.crossref_import_rejections (
    id BIGSERIAL PRIMARY KEY,
    import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id) ON DELETE CASCADE,
    source_ref TEXT,
    reason TEXT NOT NULL,
    raw_json_text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

`
	_, err := db.Exec(schema)
	if err != nil {
		return err
	}

	migrations := `
ALTER TABLE public.crossref_import_runs
    ADD COLUMN IF NOT EXISTS max_publication_date DATE;
ALTER TABLE public.crossref_import_runs
    ADD COLUMN IF NOT EXISTS max_publication_year INT;
ALTER TABLE public.crossref_works
    ADD COLUMN IF NOT EXISTS fallback_identity TEXT;
ALTER TABLE public.crossref_work_versions
    ADD COLUMN IF NOT EXISTS pub_date DATE;
ALTER TABLE public.crossref_work_versions
    ADD COLUMN IF NOT EXISTS title_norm TEXT;
ALTER TABLE public.crossref_work_versions
    ADD COLUMN IF NOT EXISTS abstract_norm TEXT;
ALTER TABLE public.crossref_work_versions
    ADD COLUMN IF NOT EXISTS text_fingerprint TEXT;
CREATE OR REPLACE VIEW public.crossref_current_works AS
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
    v.record_type,
    v.pub_date
FROM public.crossref_works w
JOIN public.crossref_work_versions v
  ON v.work_id = w.id
WHERE v.is_current;
`
	_, err = db.Exec(migrations)
	if err != nil {
		return err
	}

	indexStatements := []string{
		`
CREATE INDEX CONCURRENTLY IF NOT EXISTS crossref_works_fallback_identity_hash_idx
    ON public.crossref_works USING hash (fallback_identity)
    WHERE fallback_identity IS NOT NULL;
`,
		`
CREATE INDEX CONCURRENTLY IF NOT EXISTS crossref_works_id_normalized_doi_idx
    ON public.crossref_works (id) INCLUDE (normalized_doi)
    WHERE normalized_doi IS NOT NULL;
`,
	}
	for _, statement := range indexStatements {
		if _, err := db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func createImportRun(db *sql.DB, opts importOptions) (int64, error) {
	var runID int64
	err := db.QueryRow(
		`INSERT INTO public.crossref_import_runs (run_label, source_type, source_path, imported_by)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (run_label) DO UPDATE
         SET status = 'running',
             completed_at = NULL,
             notes = public.crossref_import_runs.notes
         WHERE public.crossref_import_runs.status = 'running'
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

func previousPublicationCutoff(db *sql.DB, runID int64) (sql.NullString, sql.NullInt64, error) {
	var maxDate sql.NullString
	var maxYear sql.NullInt64
	err := db.QueryRow(
		`SELECT max(max_publication_date)::text, max(max_publication_year)
         FROM public.crossref_import_runs
         WHERE id <> $1
           AND status = 'completed'`,
		runID,
	).Scan(&maxDate, &maxYear)
	return maxDate, maxYear, err
}

func finalizeImportRun(db *sql.DB, runID int64, status string, stats importStats) error {
	notes := fmt.Sprintf(
		"seen=%d staged=%d rejected=%d bad_json=%d missing_doi=%d missing_identity=%d version_inserts=%d batches=%d",
		stats.seen,
		stats.staged,
		stats.rejected,
		stats.badJSON,
		stats.missingDOI,
		stats.missingIdentity,
		stats.versionInserts,
		stats.batches,
	)
	_, err := db.Exec(
		`UPDATE public.crossref_import_runs
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
	lastID := int64(0)
	queryBatchSize := opts.batchSize
	if queryBatchSize < 1000 {
		queryBatchSize = 1000
	}

	for {
		currentBatchSize := queryBatchSize
		if opts.limit > 0 {
			remaining := opts.limit - processor.stats.seen
			if remaining <= 0 {
				break
			}
			if remaining < currentBatchSize {
				currentBatchSize = remaining
			}
		}

		rows, err := db.Query(
			`SELECT id, filesrc
             FROM public.raw_text_data
             WHERE id > $1
             ORDER BY id
             LIMIT $2`,
			lastID,
			currentBatchSize,
		)
		if err != nil {
			return fmt.Errorf("error reading public.raw_text_data after id %d: %w", lastID, err)
		}

		rowsInBatch := 0
		for rows.Next() {
			var rawID int64
			var raw string
			if err := rows.Scan(&rawID, &raw); err != nil {
				rows.Close()
				return fmt.Errorf("error scanning public.raw_text_data row: %w", err)
			}
			lastID = rawID
			rowsInBatch++

			processor.stats.seen++
			record, rejectReason, err := parseRecord(raw, fmt.Sprintf("%d", rawID))
			if err != nil {
				rows.Close()
				return fmt.Errorf("error parsing raw_text_data id %d: %w", rawID, err)
			}
			if rejectReason != "" {
				if err := recordRejection(processor.db, processor.runID, record.sourceRef, rejectReason, raw); err != nil {
					rows.Close()
					return err
				}
				updateRejectStats(processor.stats, rejectReason)
				if limitReached(processor.stats, opts.limit) {
					break
				}
				continue
			}
			if err := processor.add(*record); err != nil {
				rows.Close()
				return err
			}
			if limitReached(processor.stats, opts.limit) {
				break
			}
		}

		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("error iterating public.raw_text_data: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("error closing raw_text_data batch: %w", err)
		}

		if rowsInBatch == 0 || limitReached(processor.stats, opts.limit) {
			break
		}
	}

	return nil
}

func limitReached(stats *importStats, limit int) bool {
	return limit > 0 && stats.seen >= limit
}

func recordRejection(db *sql.DB, runID int64, sourceRef, reason, rawJSON string) error {
	_, err := db.Exec(
		`INSERT INTO public.crossref_import_rejections (import_run_id, source_ref, reason, raw_json_text)
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
	case "missing_doi_identity":
		stats.missingDOI++
		stats.missingIdentity++
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
	record.payloadSHA256 = sha256Hex(rawJSON)
	record.title = optionalString(firstString(payload["title"]))
	record.abstract = optionalString(extractString(payload["abstract"]))
	record.journalName = optionalString(firstString(payload["container-title"]))
	record.pubYear = extractPubYear(payload)
	record.pubDate = extractPubDate(payload)
	record.recordType = optionalString(extractString(payload["type"]))
	record.titleNorm = normalizeOptionalString(record.title)
	record.abstractNorm = normalizeOptionalString(record.abstract)
	record.textFingerprint = sha256Hex(record.titleNorm + "\x1f" + record.abstractNorm)
	record.fallbackIdentity = makeFallbackIdentity(record.journalName, record.title, record.abstract)

	if record.normalizedDOI == "" && record.fallbackIdentity == "" {
		return record, "missing_doi_identity", nil
	}

	return record, "", nil
}

func normalizeDOI(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func normalizeCrossrefText(value string) string {
	value = html.UnescapeString(value)
	value = unicodeDashReplacer.Replace(value)
	value = htmlTagPattern.ReplaceAllString(value, " ")
	value = whitespacePattern.ReplaceAllString(value, " ")
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeOptionalString(value *string) string {
	if value == nil {
		return ""
	}
	return normalizeCrossrefText(*value)
}

func makeFallbackIdentity(journalName, title, abstract *string) string {
	parts := []string{
		normalizeOptionalString(journalName),
		normalizeOptionalString(title),
		normalizeOptionalString(abstract),
	}
	for _, part := range parts {
		if part == "" {
			return ""
		}
	}
	return strings.Join(parts, "\x1f")
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

func extractPubDate(payload map[string]any) *string {
	for _, key := range []string{"published", "issued", "published-print", "published-online", "created"} {
		if date, ok := extractDateFromDateParts(payload[key]); ok {
			return &date
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

func extractDateFromDateParts(value any) (string, bool) {
	container, ok := value.(map[string]any)
	if !ok {
		return "", false
	}

	dateParts, ok := container["date-parts"].([]any)
	if !ok || len(dateParts) == 0 {
		return "", false
	}
	firstRow, ok := dateParts[0].([]any)
	if !ok || len(firstRow) == 0 {
		return "", false
	}

	year, ok := numericDatePart(firstRow[0])
	if !ok || year <= 0 {
		return "", false
	}
	month := 1
	day := 1
	if len(firstRow) > 1 {
		if parsed, ok := numericDatePart(firstRow[1]); ok && parsed >= 1 && parsed <= 12 {
			month = parsed
		}
	}
	if len(firstRow) > 2 {
		if parsed, ok := numericDatePart(firstRow[2]); ok && parsed >= 1 && parsed <= 31 {
			day = parsed
		}
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day), true
}

func numericDatePart(value any) (int, bool) {
	switch typed := value.(type) {
	case float64:
		return int(typed), true
	case int:
		return typed, true
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
    stage_id BIGSERIAL PRIMARY KEY,
    source_ref TEXT NOT NULL,
    raw_json_text TEXT NOT NULL,
    original_doi TEXT,
    normalized_doi TEXT,
    payload_sha256 TEXT NOT NULL,
    title TEXT,
    abstract TEXT,
    journal_name TEXT,
    pub_year INT,
    pub_date DATE,
    record_type TEXT,
    title_norm TEXT NOT NULL,
    abstract_norm TEXT NOT NULL,
    text_fingerprint TEXT NOT NULL,
    fallback_identity TEXT
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
		"pub_date",
		"record_type",
		"title_norm",
		"abstract_norm",
		"text_fingerprint",
		"fallback_identity",
	))
	if err != nil {
		return fmt.Errorf("error preparing COPY INTO import_stage: %w", err)
	}

	for _, record := range p.records {
		if _, err := stmt.Exec(
			record.sourceRef,
			record.rawJSON,
			nilEmptyString(record.originalDOI),
			nilEmptyString(record.normalizedDOI),
			record.payloadSHA256,
			nilString(record.title),
			nilString(record.abstract),
			nilString(record.journalName),
			nilInt(record.pubYear),
			nilString(record.pubDate),
			nilString(record.recordType),
			record.titleNorm,
			record.abstractNorm,
			record.textFingerprint,
			nilEmptyString(record.fallbackIdentity),
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

	if err := p.updateImportRunPublicationMax(tx); err != nil {
		return err
	}

	if _, err := tx.Exec(`
CREATE OR REPLACE FUNCTION pg_temp.crossref_import_normalize_text(value text)
RETURNS text
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $fn$
    SELECT lower(btrim(regexp_replace(regexp_replace(
        translate(
            replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(coalesce(value, ''),
                '&amp;', '&'),
                '&lt;', '<'),
                '&gt;', '>'),
                '&quot;', '"'),
                '&nbsp;', ' '),
                '&#160;', ' '),
                '&#xA0;', ' '),
                '&#xa0;', ' '),
                '&#x0D;', ' '),
                '&#x0d;', ' '),
                '&#13;', ' '),
                '&#x0A;', ' '),
                '&#10;', ' '),
            U&'\2010\2011\2012\2013\2014\2212',
            '------'),
        '<[^>]+>', ' ', 'g'), '\s+', ' ', 'g')));
$fn$;
`); err != nil {
		return fmt.Errorf("error creating text normalizer: %w", err)
	}

	if _, err := tx.Exec(`
CREATE TEMP TABLE stage_work_map (
    stage_id BIGINT PRIMARY KEY,
    work_id BIGINT NOT NULL
) ON COMMIT DROP;
`); err != nil {
		return fmt.Errorf("error creating stage_work_map: %w", err)
	}

	if _, err := tx.Exec(`
INSERT INTO stage_work_map (stage_id, work_id)
SELECT s.stage_id, w.id
FROM import_stage s
JOIN public.crossref_works w
  ON w.normalized_doi = s.normalized_doi
WHERE s.normalized_doi IS NOT NULL;
`); err != nil {
		return fmt.Errorf("error mapping DOI works: %w", err)
	}

	if _, err := tx.Exec(`
WITH fallback_matches AS (
    SELECT s.stage_id, min(w.id) AS work_id
    FROM import_stage s
    JOIN public.crossref_works w
      ON w.fallback_identity = s.fallback_identity
    LEFT JOIN stage_work_map existing_map
      ON existing_map.stage_id = s.stage_id
    WHERE existing_map.stage_id IS NULL
      AND s.normalized_doi IS NULL
      AND s.fallback_identity IS NOT NULL
      AND NOT (
          ($1::date IS NOT NULL AND s.pub_date IS NOT NULL AND s.pub_date > $1::date)
          OR ($1::date IS NULL AND $2::int IS NOT NULL AND s.pub_year IS NOT NULL AND s.pub_year > $2::int)
      )
    GROUP BY s.stage_id
)
INSERT INTO stage_work_map (stage_id, work_id)
SELECT stage_id, work_id
FROM fallback_matches
ON CONFLICT (stage_id) DO NOTHING;
`, nilNullString(p.previousMaxPublicationDate), nilNullInt64(p.previousMaxPublicationYear)); err != nil {
		return fmt.Errorf("error mapping fallback works: %w", err)
	}

	if _, err := tx.Exec(`
UPDATE public.crossref_works w
SET normalized_doi = COALESCE(w.normalized_doi, s.normalized_doi),
    original_doi = COALESCE(NULLIF(s.original_doi, ''), w.original_doi),
    latest_import_run_id = $1,
    updated_at = now()
FROM import_stage s
JOIN stage_work_map m
  ON m.stage_id = s.stage_id
WHERE w.id = m.work_id
  AND (
      (w.normalized_doi IS NULL AND s.normalized_doi IS NOT NULL)
      OR (w.original_doi IS NULL AND NULLIF(s.original_doi, '') IS NOT NULL)
  );
`, p.runID); err != nil {
		return fmt.Errorf("error updating mapped works: %w", err)
	}

	if _, err := tx.Exec(`
WITH unmapped_doi AS (
    SELECT DISTINCT ON (s.normalized_doi)
        s.normalized_doi,
        s.original_doi,
        s.fallback_identity
    FROM import_stage s
    LEFT JOIN stage_work_map m
      ON m.stage_id = s.stage_id
    WHERE m.stage_id IS NULL
      AND s.normalized_doi IS NOT NULL
    ORDER BY s.normalized_doi, s.stage_id DESC
)
INSERT INTO public.crossref_works (
    normalized_doi,
    original_doi,
    fallback_identity,
    first_import_run_id,
    latest_import_run_id
)
SELECT
    normalized_doi,
    original_doi,
    fallback_identity,
    $1,
    $1
FROM unmapped_doi
ON CONFLICT (normalized_doi) WHERE normalized_doi IS NOT NULL
DO UPDATE SET
    original_doi = EXCLUDED.original_doi,
    fallback_identity = COALESCE(EXCLUDED.fallback_identity, public.crossref_works.fallback_identity),
    latest_import_run_id = EXCLUDED.latest_import_run_id,
    updated_at = now();
`, p.runID); err != nil {
		return fmt.Errorf("error upserting DOI works: %w", err)
	}

	if _, err := tx.Exec(`
INSERT INTO stage_work_map (stage_id, work_id)
SELECT s.stage_id, w.id
FROM import_stage s
JOIN public.crossref_works w
  ON w.normalized_doi = s.normalized_doi
LEFT JOIN stage_work_map existing_map
  ON existing_map.stage_id = s.stage_id
WHERE existing_map.stage_id IS NULL
  AND s.normalized_doi IS NOT NULL
ON CONFLICT (stage_id) DO NOTHING;
`); err != nil {
		return fmt.Errorf("error mapping inserted DOI works: %w", err)
	}

	if _, err := tx.Exec(`
WITH same_run_matches AS (
    SELECT s.stage_id, min(w.id) AS work_id
    FROM import_stage s
    JOIN public.crossref_works w
      ON w.fallback_identity = s.fallback_identity
     AND w.first_import_run_id = $1
    LEFT JOIN stage_work_map existing_map
      ON existing_map.stage_id = s.stage_id
    WHERE existing_map.stage_id IS NULL
      AND s.normalized_doi IS NULL
      AND s.fallback_identity IS NOT NULL
    GROUP BY s.stage_id
)
INSERT INTO stage_work_map (stage_id, work_id)
SELECT stage_id, work_id
FROM same_run_matches
ON CONFLICT (stage_id) DO NOTHING;
`, p.runID); err != nil {
		return fmt.Errorf("error mapping same-run fallback works: %w", err)
	}

	if _, err := tx.Exec(`
WITH unmapped_fallback AS (
    SELECT DISTINCT ON (s.fallback_identity)
        s.fallback_identity
    FROM import_stage s
    LEFT JOIN stage_work_map m
      ON m.stage_id = s.stage_id
    WHERE m.stage_id IS NULL
      AND s.normalized_doi IS NULL
      AND s.fallback_identity IS NOT NULL
    ORDER BY s.fallback_identity, s.stage_id DESC
)
INSERT INTO public.crossref_works (
    normalized_doi,
    original_doi,
    fallback_identity,
    first_import_run_id,
    latest_import_run_id
)
SELECT
    NULL,
    NULL,
    fallback_identity,
    $1,
    $1
FROM unmapped_fallback;
`, p.runID); err != nil {
		return fmt.Errorf("error inserting fallback works: %w", err)
	}

	if _, err := tx.Exec(`
WITH fallback_matches AS (
    SELECT s.stage_id, min(w.id) AS work_id
    FROM import_stage s
    JOIN public.crossref_works w
      ON w.fallback_identity = s.fallback_identity
     AND w.first_import_run_id = $1
    LEFT JOIN stage_work_map existing_map
      ON existing_map.stage_id = s.stage_id
    WHERE existing_map.stage_id IS NULL
      AND s.normalized_doi IS NULL
      AND s.fallback_identity IS NOT NULL
    GROUP BY s.stage_id
)
INSERT INTO stage_work_map (stage_id, work_id)
SELECT stage_id, work_id
FROM fallback_matches
ON CONFLICT (stage_id) DO NOTHING;
`, p.runID); err != nil {
		return fmt.Errorf("error mapping inserted fallback works: %w", err)
	}

	var unmapped int
	if err := tx.QueryRow(`
SELECT count(*)
FROM import_stage s
LEFT JOIN stage_work_map m
  ON m.stage_id = s.stage_id
WHERE m.stage_id IS NULL;
`).Scan(&unmapped); err != nil {
		return fmt.Errorf("error checking unmapped staged rows: %w", err)
	}
	if unmapped != 0 {
		return fmt.Errorf("internal error: %d staged rows were not mapped to works", unmapped)
	}

	if _, err := tx.Exec(`
CREATE TEMP TABLE stage_resolved ON COMMIT DROP AS
SELECT
    s.stage_id,
    s.source_ref,
    s.raw_json_text,
    s.original_doi,
    s.normalized_doi,
    s.payload_sha256,
    s.title,
    s.abstract,
    s.journal_name,
    s.pub_year,
    s.pub_date,
    s.record_type,
    s.title_norm,
    s.abstract_norm,
    s.text_fingerprint,
    s.fallback_identity,
    m.work_id,
    current_v.id AS current_version_id,
    current_v.import_run_id AS current_import_run_id,
    current_v.text_fingerprint AS current_text_fingerprint,
    CASE
        WHEN current_v.id IS NULL THEN true
        WHEN current_v.text_fingerprint IS NULL THEN true
        ELSE current_v.text_fingerprint IS DISTINCT FROM s.text_fingerprint
    END AS text_changed
FROM import_stage s
JOIN stage_work_map m
  ON m.stage_id = s.stage_id
LEFT JOIN public.crossref_work_versions current_v
  ON current_v.work_id = m.work_id
 AND current_v.is_current;
`); err != nil {
		return fmt.Errorf("error creating stage_resolved: %w", err)
	}

	if _, err := tx.Exec(`
UPDATE stage_resolved sr
SET text_changed = false
FROM public.crossref_work_versions current_v
WHERE sr.current_version_id = current_v.id
  AND sr.current_text_fingerprint IS NULL
  AND current_v.title IS NOT DISTINCT FROM sr.title
  AND current_v.abstract IS NOT DISTINCT FROM sr.abstract;
`); err != nil {
		return fmt.Errorf("error marking exact legacy text matches unchanged: %w", err)
	}

	if _, err := tx.Exec(`
CREATE TEMP TABLE stage_legacy_current_text ON COMMIT DROP AS
SELECT
    sr.stage_id,
    pg_temp.crossref_import_normalize_text(current_v.title) AS current_title_norm,
    pg_temp.crossref_import_normalize_text(current_v.abstract) AS current_abstract_norm
FROM stage_resolved sr
JOIN public.crossref_work_versions current_v
  ON current_v.id = sr.current_version_id
WHERE sr.current_text_fingerprint IS NULL
  AND sr.text_changed;
`); err != nil {
		return fmt.Errorf("error normalizing legacy current text: %w", err)
	}

	if _, err := tx.Exec(`
UPDATE stage_resolved sr
SET text_changed = (
    legacy.current_title_norm IS DISTINCT FROM sr.title_norm
    OR legacy.current_abstract_norm IS DISTINCT FROM sr.abstract_norm
)
FROM stage_legacy_current_text legacy
WHERE legacy.stage_id = sr.stage_id;
`); err != nil {
		return fmt.Errorf("error applying legacy text comparison: %w", err)
	}

	if _, err := tx.Exec(`
CREATE TEMP TABLE stage_latest ON COMMIT DROP AS
SELECT DISTINCT ON (work_id)
    *
FROM stage_resolved
ORDER BY work_id, stage_id DESC;
`); err != nil {
		return fmt.Errorf("error creating stage_latest: %w", err)
	}

	if _, err := tx.Exec(`
UPDATE public.crossref_works w
SET normalized_doi = COALESCE(w.normalized_doi, s.normalized_doi),
    original_doi = COALESCE(NULLIF(s.original_doi, ''), w.original_doi),
    latest_import_run_id = $1,
    updated_at = now()
FROM stage_latest s
WHERE w.id = s.work_id
  AND s.text_changed;
`, p.runID); err != nil {
		return fmt.Errorf("error refreshing works from staged rows: %w", err)
	}

	if _, err := tx.Exec(`
CREATE TEMP TABLE stage_changes ON COMMIT DROP AS
SELECT
    s.*,
    current_v.title AS current_title,
    current_v.abstract AS current_abstract,
    COALESCE(current_v.title_norm, pg_temp.crossref_import_normalize_text(current_v.title)) AS current_title_norm,
    COALESCE(current_v.abstract_norm, pg_temp.crossref_import_normalize_text(current_v.abstract)) AS current_abstract_norm,
    current_v.payload_sha256 AS current_payload_sha256
FROM stage_latest s
LEFT JOIN public.crossref_work_versions current_v
  ON current_v.id = s.current_version_id
WHERE s.text_changed;
`); err != nil {
		return fmt.Errorf("error creating stage_changes: %w", err)
	}

	result, err := tx.Exec(`
INSERT INTO public.crossref_work_versions (
    work_id,
    import_run_id,
    raw_json_text,
    payload_sha256,
    title,
    abstract,
    journal_name,
    pub_year,
    pub_date,
    record_type,
    title_norm,
    abstract_norm,
    text_fingerprint,
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
    pub_date,
    record_type,
    title_norm,
    abstract_norm,
    text_fingerprint,
    false
FROM stage_changes
ORDER BY work_id, payload_sha256, stage_id DESC
ON CONFLICT (work_id, payload_sha256)
DO UPDATE SET
    import_run_id = EXCLUDED.import_run_id,
    raw_json_text = EXCLUDED.raw_json_text,
    title = EXCLUDED.title,
    abstract = EXCLUDED.abstract,
    journal_name = EXCLUDED.journal_name,
    pub_year = EXCLUDED.pub_year,
    pub_date = EXCLUDED.pub_date,
    record_type = EXCLUDED.record_type,
    title_norm = EXCLUDED.title_norm,
    abstract_norm = EXCLUDED.abstract_norm,
    text_fingerprint = EXCLUDED.text_fingerprint;
`, p.runID)
	if err != nil {
		return fmt.Errorf("error inserting text-changed work_versions: %w", err)
	}
	if rowsAffected, err := result.RowsAffected(); err == nil {
		p.stats.versionInserts += int(rowsAffected)
	}

	if _, err := tx.Exec(`
CREATE TEMP TABLE stage_change_versions ON COMMIT DROP AS
SELECT
    c.*,
    v.id AS new_version_id
FROM stage_changes c
JOIN public.crossref_work_versions v
  ON v.work_id = c.work_id
 AND v.payload_sha256 = c.payload_sha256;
`); err != nil {
		return fmt.Errorf("error resolving changed work_versions: %w", err)
	}

	if _, err := tx.Exec(`
INSERT INTO public.crossref_work_text_changes (
    work_id,
    from_work_version_id,
    to_work_version_id,
    from_import_run_id,
    to_import_run_id,
    previous_title,
    previous_abstract,
    new_title,
    new_abstract,
    previous_title_norm,
    previous_abstract_norm,
    new_title_norm,
    new_abstract_norm,
    previous_text_fingerprint,
    new_text_fingerprint
)
SELECT
    work_id,
    current_version_id,
    new_version_id,
    current_import_run_id,
    $1,
    current_title,
    current_abstract,
    title,
    abstract,
    current_title_norm,
    current_abstract_norm,
    title_norm,
    abstract_norm,
    current_text_fingerprint,
    text_fingerprint
FROM stage_change_versions
WHERE current_version_id IS NOT NULL
  AND (
      current_title_norm IS DISTINCT FROM title_norm
      OR current_abstract_norm IS DISTINCT FROM abstract_norm
  )
ON CONFLICT (work_id, from_work_version_id, to_work_version_id) DO NOTHING;
`, p.runID); err != nil {
		return fmt.Errorf("error inserting text change audit rows: %w", err)
	}

	if _, err := tx.Exec(`
UPDATE public.crossref_work_versions
SET is_current = false
WHERE is_current
  AND work_id IN (SELECT work_id FROM stage_changes);
`); err != nil {
		return fmt.Errorf("error clearing current flags: %w", err)
	}

	if _, err := tx.Exec(`
UPDATE public.crossref_work_versions v
SET is_current = true
FROM stage_change_versions c
WHERE v.id = c.new_version_id;
`); err != nil {
		return fmt.Errorf("error setting current flags: %w", err)
	}

	if p.enableLegacyMap {
		if _, err := tx.Exec(`
INSERT INTO public.crossref_legacy_raw_text_map (
    raw_text_data_id,
    work_id,
    work_version_id
)
SELECT DISTINCT ON (sr.source_ref)
    CAST(sr.source_ref AS BIGINT),
    sr.work_id,
    v.id
FROM stage_resolved sr
JOIN public.crossref_work_versions v
  ON v.work_id = sr.work_id
 AND v.is_current
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

func nilEmptyString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func nilNullString(value sql.NullString) any {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil
	}
	return value.String
}

func nilNullInt64(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func nilInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func (p *batchProcessor) updateImportRunPublicationMax(tx *sql.Tx) error {
	var batchMaxDate sql.NullString
	var batchMaxYear sql.NullInt64
	if err := tx.QueryRow(`
SELECT max(pub_date)::text, max(pub_year)
FROM import_stage;
`).Scan(&batchMaxDate, &batchMaxYear); err != nil {
		return fmt.Errorf("error reading staged publication maximum: %w", err)
	}

	if _, err := tx.Exec(`
UPDATE public.crossref_import_runs
SET max_publication_date = CASE
        WHEN $2::date IS NULL THEN max_publication_date
        WHEN max_publication_date IS NULL OR $2::date > max_publication_date THEN $2::date
        ELSE max_publication_date
    END,
    max_publication_year = CASE
        WHEN $3::int IS NULL THEN max_publication_year
        WHEN max_publication_year IS NULL OR $3::int > max_publication_year THEN $3::int
        ELSE max_publication_year
    END
WHERE id = $1;
`, p.runID, nilNullString(batchMaxDate), nilNullInt64(batchMaxYear)); err != nil {
		return fmt.Errorf("error updating import run publication maximum: %w", err)
	}

	return nil
}
