package crossrefjson

import "testing"

func TestParseSummaryFingerprintIgnoresMarkupChurn(t *testing.T) {
	first, err := ParseSummary([]byte(`{"DOI":"10.123/ABC","title":["A  title"],"abstract":"<jats:p>Same&#x0D; abstract</jats:p>","container-title":["Journal"]}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := ParseSummary([]byte(`{"DOI":" 10.123/abc ","title":["A title"],"abstract":"Same abstract","container-title":["Journal"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.NormalizedDOI != "10.123/abc" || second.NormalizedDOI != "10.123/abc" {
		t.Fatalf("normalized DOI mismatch: %q %q", first.NormalizedDOI, second.NormalizedDOI)
	}
	if first.TextFingerprint != second.TextFingerprint {
		t.Fatalf("fingerprints differ")
	}
}
