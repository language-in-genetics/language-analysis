package crossrefstage

import (
	"testing"
)

func TestWriterCreatesReadableStage(t *testing.T) {
	path := t.TempDir() + "/stage.sqlite"
	writer, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	if err := writer.Insert("new", "0.jsonl.gz:1", []byte(`{"DOI":"10.1/example"}`)); err != nil {
		t.Fatalf("Insert(new) error = %v", err)
	}
	if err := writer.Insert("changed", "0.jsonl.gz:2", []byte(`{"DOI":"10.2/example"}`)); err != nil {
		t.Fatalf("Insert(changed) error = %v", err)
	}
	if writer.Count("new") != 1 {
		t.Fatalf("new count = %d, want 1", writer.Count("new"))
	}
	if err := writer.Close(map[string]string{"format": "crossref-stage-sqlite"}); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db, err := OpenReadOnly(path)
	if err != nil {
		t.Fatalf("OpenReadOnly() error = %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow("SELECT count(*) FROM import_records").Scan(&count); err != nil {
		t.Fatalf("count import_records: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	var metadata string
	if err := db.QueryRow("SELECT value FROM import_metadata WHERE key = 'format'").Scan(&metadata); err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if metadata != "crossref-stage-sqlite" {
		t.Fatalf("metadata format = %q", metadata)
	}
}
