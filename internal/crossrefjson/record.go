package crossrefjson

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"html"
	"regexp"
	"strings"
)

type Summary struct {
	OriginalDOI     string
	NormalizedDOI   string
	Title           string
	Abstract        string
	Journals        []string
	TextFingerprint [32]byte
}

type rawRecord struct {
	DOI            string          `json:"DOI"`
	Title          json.RawMessage `json:"title"`
	Abstract       string          `json:"abstract"`
	ContainerTitle json.RawMessage `json:"container-title"`
}

var (
	htmlTagPattern      = regexp.MustCompile(`<[^>]+>`)
	whitespacePattern   = regexp.MustCompile(`\s+`)
	unicodeDashReplacer = strings.NewReplacer(
		"\u2010", "-",
		"\u2011", "-",
		"\u2012", "-",
		"\u2013", "-",
		"\u2014", "-",
		"\u2212", "-",
	)
)

func ParseSummary(raw []byte) (Summary, error) {
	var parsed rawRecord
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return Summary{}, err
	}
	title := firstString(parsed.Title)
	abstract := strings.TrimSpace(parsed.Abstract)
	titleNorm := NormalizeText(title)
	abstractNorm := NormalizeText(abstract)
	return Summary{
		OriginalDOI:     strings.TrimSpace(parsed.DOI),
		NormalizedDOI:   NormalizeDOI(parsed.DOI),
		Title:           title,
		Abstract:        abstract,
		Journals:        stringsFromJSON(parsed.ContainerTitle),
		TextFingerprint: FingerprintFromNormalizedText(titleNorm, abstractNorm),
	}, nil
}

func NormalizeDOI(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func NormalizeText(value string) string {
	value = html.UnescapeString(value)
	value = unicodeDashReplacer.Replace(value)
	value = htmlTagPattern.ReplaceAllString(value, " ")
	value = whitespacePattern.ReplaceAllString(value, " ")
	return strings.ToLower(strings.TrimSpace(value))
}

func FingerprintFromText(title, abstract string) [32]byte {
	return FingerprintFromNormalizedText(NormalizeText(title), NormalizeText(abstract))
}

func FingerprintFromNormalizedText(title, abstract string) [32]byte {
	return sha256.Sum256([]byte(title + "\x1f" + abstract))
}

func FingerprintHexFromText(title, abstract string) string {
	sum := FingerprintFromText(title, abstract)
	return hex.EncodeToString(sum[:])
}

func stringsFromJSON(raw json.RawMessage) []string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var values []string
	if err := json.Unmarshal(raw, &values); err == nil {
		return cleanStrings(values)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil && strings.TrimSpace(value) != "" {
		return []string{strings.TrimSpace(value)}
	}
	return nil
}

func firstString(raw json.RawMessage) string {
	values := stringsFromJSON(raw)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func cleanStrings(values []string) []string {
	cleaned := values[:0]
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			cleaned = append(cleaned, value)
		}
	}
	return cleaned
}
