package main

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
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

func TestWriterClosesReadableSQLiteBeforeDone(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "focused.sqlite")
	matches := make(chan matchRecord, 1)
	done := make(chan writerStats, 1)

	go writer(options{sqliteOut: outPath}, matches, done)
	matches <- matchRecord{
		raw:       []byte(`{"DOI":"10.1016/example","container-title":["Clinical Genetics"],"title":["Example"]}`),
		sourceRef: "0.jsonl.gz:1",
		journal:   "Clinical Genetics",
		hasDOI:    true,
	}
	close(matches)

	stats := <-done
	if stats.err != nil {
		t.Fatalf("writer err = %v", stats.err)
	}
	if stats.written != 1 {
		t.Fatalf("written = %d, want 1", stats.written)
	}

	db, err := sql.Open("sqlite", outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var category, sourceRef, raw string
	if err := db.QueryRow(`SELECT category, source_ref, raw_json_text FROM import_records`).Scan(&category, &sourceRef, &raw); err != nil {
		t.Fatal(err)
	}
	if category != "focused" {
		t.Fatalf("category = %q, want focused", category)
	}
	if sourceRef != "0.jsonl.gz:1" {
		t.Fatalf("sourceRef = %q, want 0.jsonl.gz:1", sourceRef)
	}
	if raw == "" {
		t.Fatal("empty SQLite payload")
	}
}
