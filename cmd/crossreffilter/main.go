package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"crossref-parser/internal/crossrefstage"
)

type options struct {
	dir           string
	journalsPath  string
	sqliteOut     string
	noDOIPath     string
	workers       int
	maxLineBytes  int
	progressEvery int
	requireDOI    bool
	includeNoDOI  bool
}

type journalPattern struct {
	name    string
	literal []byte
}

type crossrefHeader struct {
	DOI            string          `json:"DOI"`
	ContainerTitle json.RawMessage `json:"container-title"`
}

type matchRecord struct {
	raw       []byte
	sourceRef string
	journal   string
	hasDOI    bool
	noDOI     bool
}

type fileResult struct {
	file       string
	seen       int64
	candidates int64
	matched    int64
	noDOI      int64
	err        error
}

func main() {
	opts := parseOptions()

	journals, patterns, err := loadJournals(opts.journalsPath)
	if err != nil {
		log.Fatalf("error loading journals: %v", err)
	}
	if len(journals) == 0 {
		log.Fatal("no journals configured")
	}

	files, err := filepath.Glob(filepath.Join(opts.dir, "*.jsonl.gz"))
	if err != nil {
		log.Fatalf("error listing input files: %v", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		log.Fatalf("no *.jsonl.gz files found under %s", opts.dir)
	}

	if err := os.MkdirAll(filepath.Dir(opts.sqliteOut), 0o755); err != nil {
		log.Fatalf("error creating output directory: %v", err)
	}

	log.Printf("filtering %d files for %d journals with %d workers", len(files), len(journals), opts.workers)
	start := time.Now()

	matches := make(chan matchRecord, opts.workers*128)
	writerDone := make(chan writerStats, 1)
	go writer(opts, matches, writerDone)

	jobs := make(chan string)
	results := make(chan fileResult, len(files))
	var wg sync.WaitGroup
	for i := 0; i < opts.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range jobs {
				results <- processFile(file, opts, patterns, matches)
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
		close(matches)
	}()

	var seen, candidates, matched, noDOI int64
	completed := 0
	for result := range results {
		if result.err != nil {
			log.Fatalf("error processing %s: %v", result.file, result.err)
		}
		completed++
		seen += result.seen
		candidates += result.candidates
		matched += result.matched
		noDOI += result.noDOI
		if opts.progressEvery > 0 && (completed%opts.progressEvery == 0 || completed == len(files)) {
			rate := float64(seen) / time.Since(start).Seconds()
			log.Printf(
				"progress files=%d/%d seen=%d candidates=%d matched=%d no_doi=%d rate=%.0f lines/s",
				completed,
				len(files),
				seen,
				candidates,
				matched,
				noDOI,
				rate,
			)
		}
	}

	wstats := <-writerDone
	if wstats.err != nil {
		log.Fatalf("error writing output: %v", wstats.err)
	}

	log.Printf(
		"completed focused filter: files=%d seen=%d candidates=%d matched=%d written=%d no_doi=%d no_doi_written=%d elapsed=%s",
		len(files),
		seen,
		candidates,
		matched,
		wstats.written,
		noDOI,
		wstats.noDOIWritten,
		time.Since(start).Round(time.Second),
	)
	for _, name := range sortedKeys(wstats.byJournal) {
		log.Printf("journal_count %q %d", name, wstats.byJournal[name])
	}
}

func parseOptions() options {
	opts := options{}
	var deprecatedOut string
	flag.StringVar(&opts.dir, "dir", "", "Directory containing Crossref *.jsonl.gz files")
	flag.StringVar(&opts.journalsPath, "journals", "", "Newline-delimited journal names to keep")
	flag.StringVar(&opts.sqliteOut, "sqlite-out", "", "Focused SQLite stage output path")
	flag.StringVar(&deprecatedOut, "out", "", "Deprecated alias for -sqlite-out")
	flag.StringVar(&opts.noDOIPath, "no-doi-out", "", "Deprecated: include matched no-DOI records in the SQLite stage as no-doi")
	flag.IntVar(&opts.workers, "workers", runtime.NumCPU(), "Concurrent gzip readers")
	flag.IntVar(&opts.maxLineBytes, "max-line-bytes", 64*1024*1024, "Maximum JSON line size")
	flag.IntVar(&opts.progressEvery, "progress-every", 250, "Log progress every N completed input files")
	flag.BoolVar(&opts.requireDOI, "require-doi", false, "Write only matched records with DOI to the focused category")
	flag.BoolVar(&opts.includeNoDOI, "include-no-doi", false, "Include matched records without DOI in the SQLite stage as no-doi")
	flag.Parse()

	if opts.dir == "" {
		log.Fatal("-dir is required")
	}
	if opts.journalsPath == "" {
		log.Fatal("-journals is required")
	}
	if opts.sqliteOut == "" {
		opts.sqliteOut = deprecatedOut
	}
	if opts.sqliteOut == "" {
		log.Fatal("-sqlite-out is required")
	}
	if opts.noDOIPath != "" {
		opts.includeNoDOI = true
	}
	if opts.workers <= 0 {
		log.Fatal("-workers must be positive")
	}
	if opts.maxLineBytes <= 0 {
		log.Fatal("-max-line-bytes must be positive")
	}
	return opts
}

func loadJournals(path string) ([]string, []journalPattern, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	seen := map[string]bool{}
	var journals []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		name := strings.TrimSpace(scanner.Text())
		if name == "" || strings.HasPrefix(name, "#") {
			continue
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		journals = append(journals, name)
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	sort.Strings(journals)

	patterns := make([]journalPattern, 0, len(journals))
	for _, name := range journals {
		encoded, err := json.Marshal(name)
		if err != nil {
			return nil, nil, err
		}
		patterns = append(patterns, journalPattern{name: name, literal: encoded})
	}
	return journals, patterns, nil
}

func processFile(path string, opts options, patterns []journalPattern, matches chan<- matchRecord) fileResult {
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
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		result.seen++
		if !bytes.Contains(line, []byte(`"container-title"`)) {
			continue
		}
		if !couldMatchJournal(line, patterns) {
			continue
		}
		result.candidates++

		journal, hasDOI, ok := exactJournalMatch(line, patterns)
		if !ok {
			continue
		}
		result.matched++
		if !hasDOI {
			result.noDOI++
		}
		if opts.requireDOI && !hasDOI && !opts.includeNoDOI {
			continue
		}
		raw := make([]byte, len(line))
		copy(raw, line)
		matches <- matchRecord{
			raw:       raw,
			sourceRef: fmt.Sprintf("%s:%d", filepath.Base(path), lineNumber),
			journal:   journal,
			hasDOI:    hasDOI,
			noDOI:     !hasDOI,
		}
	}
	if err := scanner.Err(); err != nil {
		result.err = err
	}
	return result
}

func couldMatchJournal(line []byte, patterns []journalPattern) bool {
	for _, pattern := range patterns {
		if bytes.Contains(line, pattern.literal) {
			return true
		}
	}
	return false
}

func exactJournalMatch(line []byte, patterns []journalPattern) (string, bool, bool) {
	var header crossrefHeader
	if err := json.Unmarshal(line, &header); err != nil {
		return "", false, false
	}
	titles := decodeContainerTitle(header.ContainerTitle)
	if len(titles) == 0 {
		return "", strings.TrimSpace(header.DOI) != "", false
	}
	for _, title := range titles {
		for _, pattern := range patterns {
			if title == pattern.name {
				return pattern.name, strings.TrimSpace(header.DOI) != "", true
			}
		}
	}
	return "", strings.TrimSpace(header.DOI) != "", false
}

func decodeContainerTitle(raw json.RawMessage) []string {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	var titles []string
	if err := json.Unmarshal(raw, &titles); err == nil {
		return titles
	}
	var title string
	if err := json.Unmarshal(raw, &title); err == nil && title != "" {
		return []string{title}
	}
	return nil
}

type writerStats struct {
	written      int64
	noDOIWritten int64
	byJournal    map[string]int64
	err          error
}

func writer(opts options, matches <-chan matchRecord, done chan<- writerStats) {
	stats := writerStats{byJournal: map[string]int64{}}
	stage, err := crossrefstage.NewWriter(opts.sqliteOut)
	if err != nil {
		stats.err = err
		done <- stats
		return
	}
	defer stage.Abort()

	for match := range matches {
		category := "focused"
		if match.noDOI {
			if !opts.includeNoDOI {
				continue
			}
			category = "no-doi"
			stats.noDOIWritten++
		}
		if err := stage.Insert(category, match.sourceRef, match.raw); err != nil {
			stats.err = err
			break
		}
		if category == "focused" {
			stats.written++
			stats.byJournal[match.journal]++
		}
	}
	if stats.err == nil {
		stats.err = stage.Close(map[string]string{
			"format": "crossref-stage-sqlite",
			"tool":   "crossreffilter",
		})
	}
	done <- stats
}

func sortedKeys(values map[string]int64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}
