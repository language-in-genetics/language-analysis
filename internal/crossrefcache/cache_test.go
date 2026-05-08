package crossrefcache

import (
	"testing"
)

func TestWriteReadFind(t *testing.T) {
	path := t.TempDir() + "/cache.bin"
	records := []Record{
		{DOIHash: HashDOI("10.2/example"), WorkID: 2, WorkVersionID: 20},
		{DOIHash: HashDOI("10.1/example"), WorkID: 1, WorkVersionID: 10},
	}
	Sort(records)
	if err := WriteFile(path, records); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	got, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	record, ok := Find(got, HashDOI(" 10.1/EXAMPLE "))
	if !ok {
		t.Fatalf("Find() ok=false")
	}
	if record.WorkID != 1 || record.WorkVersionID != 10 {
		t.Fatalf("record = %+v", record)
	}
	if _, ok := Find(got, HashDOI("10.3/example")); ok {
		t.Fatalf("Find() returned missing DOI")
	}
}

func TestSQLiteWriterLookup(t *testing.T) {
	path := t.TempDir() + "/cache.sqlite"
	writer, err := NewSQLiteWriter(path)
	if err != nil {
		t.Fatalf("NewSQLiteWriter() error = %v", err)
	}
	records := []Record{
		{DOIHash: HashDOI("10.1/example"), TextFingerprint: HashDOI("text one"), WorkID: 1, WorkVersionID: 10},
		{DOIHash: HashDOI("10.2/example"), TextFingerprint: HashDOI("text two"), WorkID: 2, WorkVersionID: 20},
	}
	for _, record := range records {
		if err := writer.Insert(record); err != nil {
			writer.Abort()
			t.Fatalf("Insert() error = %v", err)
		}
	}
	if err := writer.Close(map[string]string{"format": "sqlite"}); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	cache, err := OpenSQLiteLookup(path, true)
	if err != nil {
		t.Fatalf("OpenSQLiteLookup() error = %v", err)
	}
	defer cache.Close()
	if cache.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", cache.Len())
	}
	record, ok, err := cache.Lookup(HashDOI(" 10.2/EXAMPLE "))
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if !ok {
		t.Fatalf("Lookup() ok=false")
	}
	if record.WorkID != 2 || record.WorkVersionID != 20 || record.TextFingerprint != HashDOI("text two") {
		t.Fatalf("record = %+v", record)
	}
	if _, ok, err := cache.Lookup(HashDOI("10.3/example")); err != nil || ok {
		t.Fatalf("missing Lookup() = ok %v err %v", ok, err)
	}
}
