package main

import (
	"strings"
	"testing"
)

func TestBuildQueryUsesBoundedDOILookup(t *testing.T) {
	query := buildQuery(true, 0)
	for _, want := range []string{
		"JOIN LATERAL",
		"crossref_works w",
		"w.id = v.work_id",
		"w.normalized_doi IS NOT NULL",
		"LIMIT 1",
		"v.title",
		"v.abstract",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("buildQuery() missing %q in:\n%s", want, query)
		}
	}
}

func TestBuildQueryLimit(t *testing.T) {
	query := buildQuery(false, 25)
	if strings.Contains(query, "v.title") || strings.Contains(query, "v.abstract") {
		t.Fatalf("buildQuery(false, ...) selected title/abstract:\n%s", query)
	}
	if !strings.HasSuffix(query, "LIMIT 25;\n") {
		t.Fatalf("buildQuery(..., 25) did not append limit:\n%s", query)
	}
}
