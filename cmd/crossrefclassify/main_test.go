package main

import (
	"testing"
)

func TestJournalMatches(t *testing.T) {
	allowed := journalSet{"Human Genetics": struct{}{}}
	if !journalMatches([]string{"Other", "Human Genetics"}, allowed) {
		t.Fatalf("journalMatches() = false, want true")
	}
	if journalMatches([]string{"Other"}, allowed) {
		t.Fatalf("journalMatches() = true, want false")
	}
}
