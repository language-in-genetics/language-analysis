package main

import (
	"database/sql"
	"fmt"
	"html/template"
	"net/url"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var targetLabelOrder = []string{
	"caucasian",
	"white",
	"european",
	"other",
	"none_of_these_labels",
}

const targetLabelOrderSQL = `CASE target_label
	WHEN 'caucasian' THEN 0
	WHEN 'white' THEN 1
	WHEN 'european' THEN 2
	WHEN 'other' THEN 3
	WHEN 'none_of_these_labels' THEN 4
	ELSE 99
END`

type Config struct {
	DBPath string
}

type BatchMeta struct {
	SampleBatch                 string
	Seed                        int
	MatchedLabelSampleSize      int
	NoneOfTheseLabelsSampleSize int
	CreatedAt                   string
	CreatedBy                   string
	SourceFilter                string
	Notes                       string
}

type TargetLabelSummary struct {
	TargetLabel    string
	TotalCount     int
	ReviewedCount  int
	PendingCount   int
	ConfirmedCount int
	DisagreedCount int
}

type AuditArticle struct {
	SampleBatch                  string
	TargetLabel                  string
	ArticleID                    int
	DOI                          string
	JournalName                  string
	PubYear                      int
	Title                        string
	Abstract                     string
	ClassifierCaucasian          bool
	ClassifierWhite              bool
	ClassifierEuropean           bool
	ClassifierOther              bool
	ClassifierEuropeanPhraseUsed string
	ClassifierOtherPhraseUsed    string
	TargetConfirmed              *bool
	ReviewerUsername             string
	ReviewNotes                  string
	ReviewedAt                   string
	UpdatedAt                    string
}

type ArticleRow struct {
	AuditArticle
	AuditOutcome string
	ReviewStatus string
}

func GetConfig() Config {
	return Config{DBPath: "../db/lig_audit.db"}
}

func OpenDatabase(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_journal_mode=WAL", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec("PRAGMA temp_store = MEMORY"); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func parseNullableBool(v sql.NullInt64) *bool {
	if !v.Valid {
		return nil
	}
	value := v.Int64 != 0
	return &value
}

func articleOutcome(article AuditArticle) string {
	if article.TargetConfirmed == nil {
		return ""
	}
	if *article.TargetConfirmed {
		return "confirmed"
	}
	return "disagreed"
}

func articleReviewStatus(article AuditArticle) string {
	if article.TargetConfirmed == nil {
		return "pending"
	}
	return "reviewed"
}

func loadCurrentBatch(db *sql.DB) (string, error) {
	var batch string
	err := db.QueryRow(`
		SELECT sample_batch
		FROM audit_batches
		ORDER BY datetime(created_at) DESC, sample_batch DESC
		LIMIT 1
	`).Scan(&batch)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return batch, err
}

func loadBatchMeta(db *sql.DB, batch string) (BatchMeta, error) {
	var meta BatchMeta
	err := db.QueryRow(`
		SELECT
			sample_batch,
			seed,
			matched_label_sample_size,
			none_of_these_labels_sample_size,
			created_at,
			COALESCE(created_by, ''),
			COALESCE(source_filter, ''),
			COALESCE(notes, '')
		FROM audit_batches
		WHERE sample_batch = ?
	`, batch).Scan(
		&meta.SampleBatch,
		&meta.Seed,
		&meta.MatchedLabelSampleSize,
		&meta.NoneOfTheseLabelsSampleSize,
		&meta.CreatedAt,
		&meta.CreatedBy,
		&meta.SourceFilter,
		&meta.Notes,
	)
	return meta, err
}

func loadTargetLabelSummaries(db *sql.DB, batch string) ([]TargetLabelSummary, error) {
	rows, err := db.Query(`
		SELECT
			target_label,
			COUNT(*) AS total_count,
			COALESCE(SUM(CASE WHEN target_confirmed IS NOT NULL THEN 1 ELSE 0 END), 0) AS reviewed_count,
			COALESCE(SUM(CASE WHEN target_confirmed IS NULL THEN 1 ELSE 0 END), 0) AS pending_count,
			COALESCE(SUM(CASE WHEN target_confirmed = 1 THEN 1 ELSE 0 END), 0) AS confirmed_count,
			COALESCE(SUM(CASE WHEN target_confirmed = 0 THEN 1 ELSE 0 END), 0) AS disagreed_count
		FROM audit_articles
		WHERE sample_batch = ?
		GROUP BY target_label
		ORDER BY `+targetLabelOrderSQL+`
	`, batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TargetLabelSummary
	for rows.Next() {
		var summary TargetLabelSummary
		if err := rows.Scan(
			&summary.TargetLabel,
			&summary.TotalCount,
			&summary.ReviewedCount,
			&summary.PendingCount,
			&summary.ConfirmedCount,
			&summary.DisagreedCount,
		); err != nil {
			return nil, err
		}
		out = append(out, summary)
	}
	return out, rows.Err()
}

func scanAuditArticle(scanner interface {
	Scan(dest ...any) error
}) (AuditArticle, error) {
	var article AuditArticle
	var classifierCaucasian int
	var classifierWhite int
	var classifierEuropean int
	var classifierOther int
	var pubYear sql.NullInt64
	var targetConfirmed sql.NullInt64
	err := scanner.Scan(
		&article.SampleBatch,
		&article.TargetLabel,
		&article.ArticleID,
		&article.DOI,
		&article.JournalName,
		&pubYear,
		&article.Title,
		&article.Abstract,
		&classifierCaucasian,
		&classifierWhite,
		&classifierEuropean,
		&classifierOther,
		&article.ClassifierEuropeanPhraseUsed,
		&article.ClassifierOtherPhraseUsed,
		&targetConfirmed,
		&article.ReviewerUsername,
		&article.ReviewNotes,
		&article.ReviewedAt,
		&article.UpdatedAt,
	)
	if err != nil {
		return AuditArticle{}, err
	}
	article.ClassifierCaucasian = classifierCaucasian != 0
	article.ClassifierWhite = classifierWhite != 0
	article.ClassifierEuropean = classifierEuropean != 0
	article.ClassifierOther = classifierOther != 0
	if pubYear.Valid {
		article.PubYear = int(pubYear.Int64)
	}
	article.TargetConfirmed = parseNullableBool(targetConfirmed)
	return article, nil
}

func loadAuditArticle(db *sql.DB, batch, targetLabel string, articleID int) (AuditArticle, error) {
	row := db.QueryRow(`
		SELECT
			sample_batch,
			target_label,
			article_id,
			COALESCE(doi, ''),
			COALESCE(journal_name, ''),
			pub_year,
			COALESCE(title, ''),
			COALESCE(abstract, ''),
			classifier_caucasian,
			classifier_white,
			classifier_european,
			classifier_other,
			COALESCE(classifier_european_phrase_used, ''),
			COALESCE(classifier_other_phrase_used, ''),
			target_confirmed,
			COALESCE(reviewer_username, ''),
			COALESCE(review_notes, ''),
			COALESCE(reviewed_at, ''),
			COALESCE(updated_at, '')
		FROM audit_articles
		WHERE sample_batch = ? AND target_label = ? AND article_id = ?
	`, batch, targetLabel, articleID)
	return scanAuditArticle(row)
}

func firstPendingArticleID(db *sql.DB, batch, targetLabel string) (int, error) {
	var articleID int
	err := db.QueryRow(`
		SELECT article_id
		FROM audit_articles
		WHERE sample_batch = ?
		  AND target_label = ?
		  AND target_confirmed IS NULL
		ORDER BY article_id
		LIMIT 1
	`, batch, targetLabel).Scan(&articleID)
	if err == sql.ErrNoRows {
		err = db.QueryRow(`
			SELECT article_id
			FROM audit_articles
			WHERE sample_batch = ?
			  AND target_label = ?
			ORDER BY article_id
			LIMIT 1
		`, batch, targetLabel).Scan(&articleID)
	}
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return articleID, err
}

func adjacentArticleIDs(db *sql.DB, batch, targetLabel string, articleID int) (int, int, error) {
	var prevID int
	var nextID int
	err := db.QueryRow(`
		SELECT COALESCE(MAX(article_id), 0)
		FROM audit_articles
		WHERE sample_batch = ?
		  AND target_label = ?
		  AND article_id < ?
	`, batch, targetLabel, articleID).Scan(&prevID)
	if err != nil {
		return 0, 0, err
	}
	err = db.QueryRow(`
		SELECT COALESCE(MIN(article_id), 0)
		FROM audit_articles
		WHERE sample_batch = ?
		  AND target_label = ?
		  AND article_id > ?
	`, batch, targetLabel, articleID).Scan(&nextID)
	return prevID, nextID, err
}

func listAuditArticles(db *sql.DB, batch, targetLabel, status string) ([]ArticleRow, error) {
	query := `
		SELECT
			sample_batch,
			target_label,
			article_id,
			COALESCE(doi, ''),
			COALESCE(journal_name, ''),
			pub_year,
			COALESCE(title, ''),
			COALESCE(abstract, ''),
			classifier_caucasian,
			classifier_white,
			classifier_european,
			classifier_other,
			COALESCE(classifier_european_phrase_used, ''),
			COALESCE(classifier_other_phrase_used, ''),
			target_confirmed,
			COALESCE(reviewer_username, ''),
			COALESCE(review_notes, ''),
			COALESCE(reviewed_at, ''),
			COALESCE(updated_at, '')
		FROM audit_articles
		WHERE sample_batch = ?
	`
	args := []any{batch}
	if targetLabel != "" {
		query += " AND target_label = ?"
		args = append(args, targetLabel)
	}
	if status == "pending" {
		query += " AND target_confirmed IS NULL"
	} else if status == "reviewed" {
		query += " AND target_confirmed IS NOT NULL"
	}
	query += " ORDER BY " + targetLabelOrderSQL + ", article_id"

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ArticleRow
	for rows.Next() {
		article, err := scanAuditArticle(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ArticleRow{
			AuditArticle: article,
			AuditOutcome: articleOutcome(article),
			ReviewStatus: articleReviewStatus(article),
		})
	}
	return out, rows.Err()
}

func formatTimestamp(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05-07:00",
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.Format("2006-01-02 15:04")
		}
	}
	return raw
}

func phraseList(article AuditArticle) []string {
	parts := []string{}
	if article.ClassifierCaucasian {
		parts = append(parts, "caucasian")
	}
	if article.ClassifierWhite {
		parts = append(parts, "white")
	}
	if article.ClassifierEuropean {
		if article.ClassifierEuropeanPhraseUsed != "" {
			parts = append(parts, "european: "+article.ClassifierEuropeanPhraseUsed)
		} else {
			parts = append(parts, "european")
		}
	}
	if article.ClassifierOther {
		if article.ClassifierOtherPhraseUsed != "" {
			parts = append(parts, "other: "+article.ClassifierOtherPhraseUsed)
		} else {
			parts = append(parts, "other")
		}
	}
	if len(parts) == 0 {
		return []string{"none"}
	}
	return parts
}

func buttonTargetLabel(targetLabel string) string {
	switch targetLabel {
	case "caucasian":
		return "Caucasian"
	case "white":
		return "White"
	case "european":
		return "European"
	case "other":
		return "Other"
	case "none_of_these_labels":
		return "None Of These Labels"
	default:
		return targetLabel
	}
}

func targetLabelDisplay(targetLabel string) string {
	switch targetLabel {
	case "caucasian":
		return "caucasian"
	case "white":
		return "white"
	case "european":
		return "european"
	case "other":
		return "other"
	case "none_of_these_labels":
		return "none of these labels"
	default:
		return targetLabel
	}
}

func confirmButtonLabel(targetLabel string) string {
	if targetLabel == "none_of_these_labels" {
		return "Mark None Of These Labels"
	}
	return "Mark " + buttonTargetLabel(targetLabel)
}

func rejectButtonLabel(targetLabel string) string {
	if targetLabel == "none_of_these_labels" {
		return "Mark Uses Tracked Labels"
	}
	return "Mark Not " + buttonTargetLabel(targetLabel)
}

func targetLabelSummaryLabel(summary TargetLabelSummary) string {
	if summary.TargetLabel == "none_of_these_labels" {
		return fmt.Sprintf(
			"Confirmed none of these labels %d · Uses tracked labels on review %d",
			summary.ConfirmedCount,
			summary.DisagreedCount,
		)
	}
	label := targetLabelDisplay(summary.TargetLabel)
	return fmt.Sprintf(
		"Confirmed %s %d · Not %s on review %d",
		label,
		summary.ConfirmedCount,
		label,
		summary.DisagreedCount,
	)
}

func outcomeLabelForTarget(targetLabel, outcome string) string {
	if outcome == "" {
		return "pending"
	}
	if targetLabel == "none_of_these_labels" {
		if outcome == "confirmed" {
			return "confirmed none of these labels"
		}
		return "uses tracked labels on review"
	}
	label := targetLabelDisplay(targetLabel)
	if outcome == "confirmed" {
		return "confirmed " + label
	}
	return "not " + label + " on review"
}

func queryWithBatch(batch string) string {
	return "?batch=" + url.QueryEscape(batch)
}

func defaultTargetLabel(summaries []TargetLabelSummary) string {
	for _, targetLabel := range targetLabelOrder {
		for _, summary := range summaries {
			if summary.TargetLabel == targetLabel {
				return targetLabel
			}
		}
	}
	if len(summaries) > 0 {
		return summaries[0].TargetLabel
	}
	return "white"
}

var templateFuncs = template.FuncMap{
	"articleOutcome": func(article AuditArticle) string {
		return articleOutcome(article)
	},
	"articleReviewStatus": func(article AuditArticle) string {
		return articleReviewStatus(article)
	},
	"confirmButtonLabel": confirmButtonLabel,
	"formatTimestamp":    formatTimestamp,
	"joinPhrases": func(article AuditArticle) string {
		return strings.Join(phraseList(article), ", ")
	},
	"outcomeLabel": func(targetLabel, outcome string) string {
		return outcomeLabelForTarget(targetLabel, outcome)
	},
	"queryWithBatch":          queryWithBatch,
	"rejectButtonLabel":       rejectButtonLabel,
	"targetLabelDisplay":      targetLabelDisplay,
	"targetLabelSummaryLabel": targetLabelSummaryLabel,
	"yearLabel": func(year int) string {
		if year == 0 {
			return "—"
		}
		return fmt.Sprintf("%d", year)
	},
}
