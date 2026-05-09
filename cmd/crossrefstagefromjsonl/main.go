package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"crossref-parser/internal/crossrefstage"
)

type options struct {
	dir           string
	outPath       string
	categories    []string
	maxLineBytes  int
	reportEvery   int
	allowMissing  bool
	metadataLabel string
}

func main() {
	opts := parseOptions()
	start := time.Now()

	writer, err := crossrefstage.NewWriter(opts.outPath)
	if err != nil {
		log.Fatalf("error creating SQLite stage: %v", err)
	}
	defer writer.Abort()

	var total int64
	for _, category := range opts.categories {
		path := filepath.Join(opts.dir, category+".jsonl.gz")
		count, err := copyCategory(writer, category, path, opts, start, &total)
		if err != nil {
			if opts.allowMissing && os.IsNotExist(err) {
				log.Printf("skipping missing %s", path)
				continue
			}
			log.Fatalf("error converting %s: %v", path, err)
		}
		log.Printf("converted category=%s rows=%d", category, count)
	}

	if err := writer.Close(map[string]string{
		"format":       "crossref-stage-sqlite",
		"tool":         "crossrefstagefromjsonl",
		"source_dir":   opts.dir,
		"source_label": opts.metadataLabel,
	}); err != nil {
		log.Fatalf("error closing SQLite stage: %v", err)
	}
	log.Printf("completed SQLite stage %s rows=%d elapsed=%s", opts.outPath, total, time.Since(start).Round(time.Second))
}

func parseOptions() options {
	opts := options{}
	var categories string
	flag.StringVar(&opts.dir, "dir", "", "Directory containing legacy classified JSONL gzip files")
	flag.StringVar(&opts.outPath, "out", "", "SQLite stage output path")
	flag.StringVar(&categories, "categories", "new,changed,unknown-fingerprint,no-doi", "Comma-separated category names to read from DIR/CATEGORY.jsonl.gz")
	flag.IntVar(&opts.maxLineBytes, "max-line-bytes", 64*1024*1024, "Maximum JSON line size")
	flag.IntVar(&opts.reportEvery, "report-every", 100000, "Log progress every N written rows")
	flag.BoolVar(&opts.allowMissing, "allow-missing", true, "Skip missing category files")
	flag.StringVar(&opts.metadataLabel, "source-label", "", "Optional source label stored in SQLite metadata")
	flag.Parse()

	if opts.dir == "" {
		log.Fatal("-dir is required")
	}
	if opts.outPath == "" {
		log.Fatal("-out is required")
	}
	opts.categories = splitCategories(categories)
	if len(opts.categories) == 0 {
		log.Fatal("-categories must include at least one category")
	}
	if opts.maxLineBytes <= 0 {
		log.Fatal("-max-line-bytes must be positive")
	}
	if opts.reportEvery < 0 {
		log.Fatal("-report-every must be non-negative")
	}
	return opts
}

func splitCategories(value string) []string {
	seen := map[string]struct{}{}
	var categories []string
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		categories = append(categories, item)
	}
	return categories
}

func copyCategory(writer *crossrefstage.Writer, category, path string, opts options, start time.Time, total *int64) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return 0, err
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)
	scanner.Buffer(make([]byte, 1024*1024), opts.maxLineBytes)
	var count int64
	lineNumber := int64(0)
	for scanner.Scan() {
		lineNumber++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		raw := make([]byte, len(line))
		copy(raw, line)
		sourceRef := fmt.Sprintf("%s:%d", filepath.Base(path), lineNumber)
		if err := writer.Insert(category, sourceRef, raw); err != nil {
			return count, err
		}
		count++
		*total = *total + 1
		if opts.reportEvery > 0 && *total%int64(opts.reportEvery) == 0 {
			rate := float64(*total) / time.Since(start).Seconds()
			log.Printf("progress rows=%d category=%s category_rows=%d rate=%.0f rows/s", *total, category, count, rate)
		}
	}
	if err := scanner.Err(); err != nil {
		return count, err
	}
	return count, nil
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}
