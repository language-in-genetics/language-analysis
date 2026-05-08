package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"crossref-parser/internal/crossrefcache"
	"crossref-parser/internal/crossrefjson"
)

type options struct {
	cachePath          string
	cacheFormat        string
	sqliteCopyToMemory bool
	dir                string
	outDir             string
	journalsPath       string
	workers            int
	maxLineBytes       int
	progressEvery      int
	limit              int
}

type journalSet map[string]struct{}

type classifiedRecord struct {
	category string
	raw      []byte
}

type fileResult struct {
	file      string
	seen      int64
	newDOI    int64
	changed   int64
	unchanged int64
	unknown   int64
	noDOI     int64
	filtered  int64
	badJSON   int64
	err       error
}

type writerStats struct {
	newDOI  int64
	changed int64
	unknown int64
	noDOI   int64
	err     error
}

type lookupCache interface {
	Len() int
	Lookup([32]byte) (crossrefcache.Record, bool, error)
	Close() error
}

type binaryLookupCache struct {
	records []crossrefcache.Record
}

func main() {
	opts := parseOptions()
	start := time.Now()

	log.Printf("loading %s DOI cache from %s", opts.cacheFormat, opts.cachePath)
	cache, err := openLookupCache(opts)
	if err != nil {
		log.Fatalf("error reading DOI cache: %v", err)
	}
	defer cache.Close()
	log.Printf("loaded %d DOI cache records in %s", cache.Len(), time.Since(start).Round(time.Second))

	journals, err := loadJournalSet(opts.journalsPath)
	if err != nil {
		log.Fatalf("error loading journals: %v", err)
	}
	if journals != nil {
		log.Printf("restricting classification to %d journals", len(journals))
	}

	files, err := filepath.Glob(filepath.Join(opts.dir, "*.jsonl.gz"))
	if err != nil {
		log.Fatalf("error listing input files: %v", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		log.Fatalf("no *.jsonl.gz files found under %s", opts.dir)
	}
	if opts.limit > 0 && opts.limit < len(files) {
		files = files[:opts.limit]
	}
	if err := os.MkdirAll(opts.outDir, 0o755); err != nil {
		log.Fatalf("error creating output directory: %v", err)
	}

	records := make(chan classifiedRecord, opts.workers*128)
	writerDone := make(chan writerStats, 1)
	go writer(opts.outDir, records, writerDone)

	jobs := make(chan string)
	results := make(chan fileResult, len(files))
	var wg sync.WaitGroup
	for i := 0; i < opts.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				results <- processFile(path, opts, cache, journals, records)
			}
		}()
	}

	go func() {
		for _, file := range files {
			jobs <- file
		}
		close(jobs)
		wg.Wait()
		close(results)
		close(records)
	}()

	var total fileResult
	completed := 0
	for result := range results {
		if result.err != nil {
			log.Fatalf("error processing %s: %v", result.file, result.err)
		}
		completed++
		total.seen += result.seen
		total.newDOI += result.newDOI
		total.changed += result.changed
		total.unchanged += result.unchanged
		total.unknown += result.unknown
		total.noDOI += result.noDOI
		total.filtered += result.filtered
		total.badJSON += result.badJSON
		if opts.progressEvery > 0 && (completed%opts.progressEvery == 0 || completed == len(files)) {
			rate := float64(total.seen) / time.Since(start).Seconds()
			log.Printf(
				"progress files=%d/%d seen=%d new=%d changed=%d unchanged=%d unknown_fingerprint=%d no_doi=%d filtered=%d bad_json=%d rate=%.0f lines/s",
				completed,
				len(files),
				total.seen,
				total.newDOI,
				total.changed,
				total.unchanged,
				total.unknown,
				total.noDOI,
				total.filtered,
				total.badJSON,
				rate,
			)
		}
	}

	wstats := <-writerDone
	if wstats.err != nil {
		log.Fatalf("error writing classified output: %v", wstats.err)
	}
	if err := writeSummary(opts.outDir, total, wstats, len(files), cache.Len(), time.Since(start)); err != nil {
		log.Fatalf("error writing summary: %v", err)
	}
	log.Printf(
		"completed classification: files=%d seen=%d new=%d changed=%d unchanged=%d unknown_fingerprint=%d no_doi=%d filtered=%d bad_json=%d elapsed=%s",
		len(files),
		total.seen,
		total.newDOI,
		total.changed,
		total.unchanged,
		total.unknown,
		total.noDOI,
		total.filtered,
		total.badJSON,
		time.Since(start).Round(time.Second),
	)
}

func parseOptions() options {
	opts := options{}
	flag.StringVar(&opts.cachePath, "cache", "", "Compact DOI cache path")
	flag.StringVar(&opts.cacheFormat, "cache-format", "auto", "Cache format: auto, binary, or sqlite")
	flag.BoolVar(&opts.sqliteCopyToMemory, "sqlite-copy-to-memory", true, "Copy SQLite cache into an in-memory database before classification")
	flag.StringVar(&opts.dir, "dir", "", "Directory containing Crossref *.jsonl.gz files")
	flag.StringVar(&opts.outDir, "out-dir", "", "Directory for classified JSONL gzip outputs")
	flag.StringVar(&opts.journalsPath, "journals", "", "Optional newline-delimited journal names to keep")
	flag.IntVar(&opts.workers, "workers", runtime.NumCPU(), "Concurrent gzip readers")
	flag.IntVar(&opts.maxLineBytes, "max-line-bytes", 64*1024*1024, "Maximum JSON line size")
	flag.IntVar(&opts.progressEvery, "progress-every", 250, "Log progress every N completed input files")
	flag.IntVar(&opts.limit, "limit-files", 0, "Stop after N input files, for smoke tests")
	flag.Parse()

	if opts.cachePath == "" {
		log.Fatal("-cache is required")
	}
	opts.cacheFormat = inferCacheFormat(opts.cachePath, opts.cacheFormat)
	if opts.dir == "" {
		log.Fatal("-dir is required")
	}
	if opts.outDir == "" {
		log.Fatal("-out-dir is required")
	}
	if opts.workers <= 0 {
		log.Fatal("-workers must be positive")
	}
	return opts
}

func inferCacheFormat(path, format string) string {
	if format != "auto" {
		if format != "binary" && format != "sqlite" {
			log.Fatal("-cache-format must be auto, binary, or sqlite")
		}
		return format
	}
	lower := strings.ToLower(path)
	if strings.HasSuffix(lower, ".sqlite") || strings.HasSuffix(lower, ".sqlite3") || strings.HasSuffix(lower, ".db") {
		return "sqlite"
	}
	return "binary"
}

func openLookupCache(opts options) (lookupCache, error) {
	switch opts.cacheFormat {
	case "binary":
		records, err := crossrefcache.ReadFile(opts.cachePath)
		if err != nil {
			return nil, err
		}
		return binaryLookupCache{records: records}, nil
	case "sqlite":
		return crossrefcache.OpenSQLiteLookup(opts.cachePath, opts.sqliteCopyToMemory)
	default:
		return nil, os.ErrInvalid
	}
}

func (c binaryLookupCache) Len() int {
	return len(c.records)
}

func (c binaryLookupCache) Lookup(doiHash [32]byte) (crossrefcache.Record, bool, error) {
	record, ok := crossrefcache.Find(c.records, doiHash)
	return record, ok, nil
}

func (c binaryLookupCache) Close() error {
	return nil
}

func processFile(path string, opts options, cache lookupCache, journals journalSet, output chan<- classifiedRecord) fileResult {
	result := fileResult{file: path}
	file, err := os.Open(path)
	if err != nil {
		result.err = err
		return result
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		result.err = err
		return result
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)
	scanner.Buffer(make([]byte, 1024*1024), opts.maxLineBytes)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		result.seen++
		summary, err := crossrefjson.ParseSummary(line)
		if err != nil {
			result.badJSON++
			continue
		}
		if journals != nil && !journalMatches(summary.Journals, journals) {
			result.filtered++
			continue
		}
		raw := make([]byte, len(line))
		copy(raw, line)
		if summary.NormalizedDOI == "" {
			result.noDOI++
			output <- classifiedRecord{category: "no-doi", raw: raw}
			continue
		}
		record, ok, err := cache.Lookup(crossrefcache.HashDOI(summary.NormalizedDOI))
		if err != nil {
			result.err = err
			return result
		}
		if !ok {
			result.newDOI++
			output <- classifiedRecord{category: "new", raw: raw}
			continue
		}
		if record.TextFingerprint == summary.TextFingerprint {
			result.unchanged++
			continue
		}
		if record.TextFingerprint == ([32]byte{}) {
			result.unknown++
			output <- classifiedRecord{category: "unknown-fingerprint", raw: raw}
			continue
		}
		result.changed++
		output <- classifiedRecord{category: "changed", raw: raw}
	}
	if err := scanner.Err(); err != nil {
		result.err = err
	}
	return result
}

func writer(outDir string, records <-chan classifiedRecord, done chan<- writerStats) {
	stats := writerStats{}
	files := map[string]*gzip.Writer{}
	closers := []func() error{}
	open := func(category string) (*gzip.Writer, error) {
		if writer, ok := files[category]; ok {
			return writer, nil
		}
		path := filepath.Join(outDir, category+".jsonl.gz")
		file, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		buffered := bufio.NewWriterSize(file, 4*1024*1024)
		gz, err := gzip.NewWriterLevel(buffered, gzip.BestSpeed)
		if err != nil {
			file.Close()
			return nil, err
		}
		files[category] = gz
		closers = append(closers, gz.Close, buffered.Flush, file.Close)
		return gz, nil
	}

	for record := range records {
		gz, err := open(record.category)
		if err != nil {
			stats.err = err
			break
		}
		if _, err := gz.Write(record.raw); err != nil {
			stats.err = err
			break
		}
		if _, err := gz.Write([]byte{'\n'}); err != nil {
			stats.err = err
			break
		}
		switch record.category {
		case "new":
			stats.newDOI++
		case "changed":
			stats.changed++
		case "unknown-fingerprint":
			stats.unknown++
		case "no-doi":
			stats.noDOI++
		}
	}
	for _, closeFn := range closers {
		if err := closeFn(); err != nil && stats.err == nil {
			stats.err = err
		}
	}
	done <- stats
}

func loadJournalSet(path string) (journalSet, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	values := journalSet{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		value := strings.TrimSpace(scanner.Text())
		if value == "" || strings.HasPrefix(value, "#") {
			continue
		}
		values[value] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func journalMatches(journals []string, allowed journalSet) bool {
	for _, journal := range journals {
		if _, ok := allowed[journal]; ok {
			return true
		}
	}
	return false
}

func writeSummary(outDir string, total fileResult, written writerStats, files int, cacheRecords int, elapsed time.Duration) error {
	value := map[string]any{
		"files":         files,
		"cache_records": cacheRecords,
		"seen":          total.seen,
		"new":           total.newDOI,
		"changed":       total.changed,
		"unchanged":     total.unchanged,
		"unknown":       total.unknown,
		"no_doi":        total.noDOI,
		"filtered":      total.filtered,
		"bad_json":      total.badJSON,
		"written": map[string]int64{
			"new":     written.newDOI,
			"changed": written.changed,
			"unknown": written.unknown,
			"no_doi":  written.noDOI,
		},
		"elapsed": elapsed.Round(time.Second).String(),
	}
	file, err := os.Create(filepath.Join(outDir, "summary.json"))
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
