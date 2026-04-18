package main

import "testing"

func TestNormalizeDOI(t *testing.T) {
	got := normalizeDOI(" 10.1016/ABC-123 ")
	if got != "10.1016/abc-123" {
		t.Fatalf("normalizeDOI() = %q, want %q", got, "10.1016/abc-123")
	}
}

func TestParseRecordMissingDOI(t *testing.T) {
	record, rejectReason, err := parseRecord(`{"title":["Example"]}`, "file:1")
	if err != nil {
		t.Fatalf("parseRecord() error = %v", err)
	}
	if rejectReason != "missing_doi" {
		t.Fatalf("parseRecord() rejectReason = %q, want %q", rejectReason, "missing_doi")
	}
	if record.sourceRef != "file:1" {
		t.Fatalf("sourceRef = %q, want %q", record.sourceRef, "file:1")
	}
}

func TestParseRecordExtractsFields(t *testing.T) {
	raw := `{
	  "DOI": "10.1016/0012-365x(92)00483-8",
	  "title": ["A title"],
	  "container-title": ["Discrete Mathematics"],
	  "abstract": "<jats:p>Abstract</jats:p>",
	  "type": "journal-article",
	  "published": {"date-parts": [[1992, 1, 1]]}
	}`

	record, rejectReason, err := parseRecord(raw, "0.jsonl.gz:1")
	if err != nil {
		t.Fatalf("parseRecord() error = %v", err)
	}
	if rejectReason != "" {
		t.Fatalf("parseRecord() rejectReason = %q, want empty", rejectReason)
	}
	if record.normalizedDOI != "10.1016/0012-365x(92)00483-8" {
		t.Fatalf("normalizedDOI = %q", record.normalizedDOI)
	}
	if record.title == nil || *record.title != "A title" {
		t.Fatalf("title = %#v", record.title)
	}
	if record.journalName == nil || *record.journalName != "Discrete Mathematics" {
		t.Fatalf("journalName = %#v", record.journalName)
	}
	if record.pubYear == nil || *record.pubYear != 1992 {
		t.Fatalf("pubYear = %#v", record.pubYear)
	}
}

func TestExtractPubYearFallsBackToIssued(t *testing.T) {
	payload := map[string]any{
		"issued": map[string]any{
			"date-parts": []any{
				[]any{float64(1937), float64(1), float64(1)},
			},
		},
	}
	year := extractPubYear(payload)
	if year == nil || *year != 1937 {
		t.Fatalf("extractPubYear() = %#v, want 1937", year)
	}
}
