package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestExactJournalMatch(t *testing.T) {
	journal := "The American Journal of Human Genetics"
	encoded, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	patterns := []journalPattern{{name: journal, literal: encoded}}
	line := []byte(`{"DOI":"10.1016/example","container-title":["The American Journal of Human Genetics"],"title":["Example"]}`)

	gotJournal, hasDOI, ok := exactJournalMatch(line, patterns)
	if !ok {
		t.Fatalf("exactJournalMatch ok=false")
	}
	if gotJournal != journal {
		t.Fatalf("journal = %q, want %q", gotJournal, journal)
	}
	if !hasDOI {
		t.Fatalf("hasDOI=false, want true")
	}
}

func TestExactJournalMatchRejectsFalseLiteralHit(t *testing.T) {
	journal := "Human Genetics"
	encoded, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	patterns := []journalPattern{{name: journal, literal: encoded}}
	line := []byte(`{"DOI":"10.1016/example","container-title":["Other Journal"],"title":["Human Genetics"]}`)

	_, _, ok := exactJournalMatch(line, patterns)
	if ok {
		t.Fatalf("exactJournalMatch accepted a non-container-title literal hit")
	}
}

func TestExactJournalMatchDetectsMissingDOI(t *testing.T) {
	journal := "Clinical Genetics"
	encoded, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	patterns := []journalPattern{{name: journal, literal: encoded}}
	line := []byte(`{"container-title":["Clinical Genetics"],"title":["Example"]}`)

	gotJournal, hasDOI, ok := exactJournalMatch(line, patterns)
	if !ok || gotJournal != journal {
		t.Fatalf("exactJournalMatch = %q, %v; want %q, true", gotJournal, ok, journal)
	}
	if hasDOI {
		t.Fatalf("hasDOI=true, want false")
	}
}

func TestWriterClosesReadableGzipBeforeDone(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "focused.jsonl.gz")
	matches := make(chan matchRecord, 1)
	done := make(chan writerStats, 1)

	go writer(options{outPath: outPath}, matches, done)
	matches <- matchRecord{
		raw:     []byte(`{"DOI":"10.1016/example","container-title":["Clinical Genetics"],"title":["Example"]}`),
		journal: "Clinical Genetics",
		hasDOI:  true,
	}
	close(matches)

	stats := <-done
	if stats.err != nil {
		t.Fatalf("writer err = %v", stats.err)
	}
	if stats.written != 1 {
		t.Fatalf("written = %d, want 1", stats.written)
	}

	file, err := os.Open(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gzReader.Close()
	reader := bufio.NewReader(gzReader)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	if line == "" {
		t.Fatal("empty gzip payload")
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatalf("reading gzip to EOF: %v", err)
	}
}
