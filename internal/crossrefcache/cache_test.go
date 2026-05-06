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
