package main

import "testing"

func TestNormalizeDOI(t *testing.T) {
	got := normalizeDOI(" 10.1016/ABC-123 ")
	if got != "10.1016/abc-123" {
		t.Fatalf("normalizeDOI() = %q, want %q", got, "10.1016/abc-123")
	}
}

func TestParseRecordMissingDOIAndFallbackIdentity(t *testing.T) {
	record, rejectReason, err := parseRecord(`{"title":["Example"]}`, "file:1")
	if err != nil {
		t.Fatalf("parseRecord() error = %v", err)
	}
	if rejectReason != "missing_doi_identity" {
		t.Fatalf("parseRecord() rejectReason = %q, want %q", rejectReason, "missing_doi_identity")
	}
	if record.sourceRef != "file:1" {
		t.Fatalf("sourceRef = %q, want %q", record.sourceRef, "file:1")
	}
}

func TestParseRecordMissingDOIWithFallbackIdentity(t *testing.T) {
	raw := `{
	  "title": ["A title"],
	  "container-title": ["Discrete Mathematics"],
	  "abstract": "<jats:p>An abstract</jats:p>",
	  "type": "journal-article",
	  "published": {"date-parts": [[2026, 3, 15]]}
	}`

	record, rejectReason, err := parseRecord(raw, "0.jsonl.gz:1")
	if err != nil {
		t.Fatalf("parseRecord() error = %v", err)
	}
	if rejectReason != "" {
		t.Fatalf("parseRecord() rejectReason = %q, want empty", rejectReason)
	}
	if record.normalizedDOI != "" {
		t.Fatalf("normalizedDOI = %q, want empty", record.normalizedDOI)
	}
	if record.fallbackIdentity == "" {
		t.Fatalf("fallbackIdentity is empty")
	}
	if record.pubDate == nil || *record.pubDate != "2026-03-15" {
		t.Fatalf("pubDate = %#v, want 2026-03-15", record.pubDate)
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
	if record.pubDate == nil || *record.pubDate != "1992-01-01" {
		t.Fatalf("pubDate = %#v, want 1992-01-01", record.pubDate)
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

func TestTextFingerprintIgnoresPayloadChurn(t *testing.T) {
	first := `{
	  "DOI": "10.123/example",
	  "title": ["A  title"],
	  "container-title": ["Example Journal"],
	  "abstract": "<jats:p>Same abstract</jats:p>",
	  "indexed": {"date-time": "2025-01-01T00:00:00Z"}
	}`
	second := `{
	  "DOI": "10.123/example",
	  "title": ["A title"],
	  "container-title": ["Example Journal"],
	  "abstract": "Same abstract",
	  "indexed": {"date-time": "2026-01-01T00:00:00Z"}
	}`

	firstRecord, rejectReason, err := parseRecord(first, "first")
	if err != nil || rejectReason != "" {
		t.Fatalf("first parse err=%v reject=%q", err, rejectReason)
	}
	secondRecord, rejectReason, err := parseRecord(second, "second")
	if err != nil || rejectReason != "" {
		t.Fatalf("second parse err=%v reject=%q", err, rejectReason)
	}
	if firstRecord.payloadSHA256 == secondRecord.payloadSHA256 {
		t.Fatalf("payloadSHA256 should differ when raw JSON metadata changes")
	}
	if firstRecord.textFingerprint != secondRecord.textFingerprint {
		t.Fatalf("textFingerprint differs for equivalent title/abstract text")
	}
}

func TestNormalizeCrossrefTextHandlesLegacyMarkupChurn(t *testing.T) {
	first := "<jats:title>Abstract</jats:title>\n<jats:p>Post-bariatric editorial&#x0D; Biblioteca &amp; Archive</jats:p>"
	second := "<title>Abstract</title> <p>Post\u2010bariatric editorial\nBiblioteca & Archive</p>"

	if normalizeCrossrefText(first) != normalizeCrossrefText(second) {
		t.Fatalf("normalizeCrossrefText() should treat equivalent markup/entity churn as equal:\nfirst:  %q\nsecond: %q", normalizeCrossrefText(first), normalizeCrossrefText(second))
	}
}
