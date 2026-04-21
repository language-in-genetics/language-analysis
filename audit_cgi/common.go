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

type Config struct {
	DBPath string
}

type BatchMeta struct {
	SampleBatch        string
	Seed               int
	PositiveSampleSize int
	NegativeSampleSize int
	CreatedAt          string
	CreatedBy          string
	SourceFilter       string
	Notes              string
}

type GroupSummary struct {
	SampleGroup       string
	TotalCount        int
	ReviewedCount     int
	PendingCount      int
	TruePositiveCount int
	FalsePositiveCount int
	TrueNegativeCount int
	FalseNegativeCount int
}

type AuditArticle struct {
	SampleBatch                  string
	SampleGroup                  string
	ArticleID                    int
	PredictedPositive            bool
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
	HumanPositive                *bool
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
	if article.HumanPositive == nil {
		return ""
	}
	switch {
	case article.PredictedPositive && *article.HumanPositive:
		return "true_positive"
	case article.PredictedPositive && !*article.HumanPositive:
		return "false_positive"
	case !article.PredictedPositive && *article.HumanPositive:
		return "false_negative"
	default:
		return "true_negative"
	}
}

func articleReviewStatus(article AuditArticle) string {
	if article.HumanPositive == nil {
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
			positive_sample_size,
			negative_sample_size,
			created_at,
			COALESCE(created_by, ''),
			COALESCE(source_filter, ''),
			COALESCE(notes, '')
		FROM audit_batches
		WHERE sample_batch = ?
	`, batch).Scan(
		&meta.SampleBatch,
		&meta.Seed,
		&meta.PositiveSampleSize,
		&meta.NegativeSampleSize,
		&meta.CreatedAt,
		&meta.CreatedBy,
		&meta.SourceFilter,
		&meta.Notes,
	)
	return meta, err
}

func loadGroupSummaries(db *sql.DB, batch string) ([]GroupSummary, error) {
	rows, err := db.Query(`
		SELECT
			sample_group,
			COUNT(*) AS total_count,
			COALESCE(SUM(CASE WHEN human_positive IS NOT NULL THEN 1 ELSE 0 END), 0) AS reviewed_count,
			COALESCE(SUM(CASE WHEN human_positive IS NULL THEN 1 ELSE 0 END), 0) AS pending_count,
			COALESCE(SUM(CASE WHEN predicted_positive = 1 AND human_positive = 1 THEN 1 ELSE 0 END), 0) AS true_positive_count,
			COALESCE(SUM(CASE WHEN predicted_positive = 1 AND human_positive = 0 THEN 1 ELSE 0 END), 0) AS false_positive_count,
			COALESCE(SUM(CASE WHEN predicted_positive = 0 AND human_positive = 0 THEN 1 ELSE 0 END), 0) AS true_negative_count,
			COALESCE(SUM(CASE WHEN predicted_positive = 0 AND human_positive = 1 THEN 1 ELSE 0 END), 0) AS false_negative_count
		FROM audit_articles
		WHERE sample_batch = ?
		GROUP BY sample_group
		ORDER BY CASE sample_group WHEN 'positive' THEN 0 ELSE 1 END, sample_group
	`, batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []GroupSummary
	for rows.Next() {
		var summary GroupSummary
		if err := rows.Scan(
			&summary.SampleGroup,
			&summary.TotalCount,
			&summary.ReviewedCount,
			&summary.PendingCount,
			&summary.TruePositiveCount,
			&summary.FalsePositiveCount,
			&summary.TrueNegativeCount,
			&summary.FalseNegativeCount,
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
	var predictedPositive int
	var classifierCaucasian int
	var classifierWhite int
	var classifierEuropean int
	var classifierOther int
	var pubYear sql.NullInt64
	var humanPositive sql.NullInt64
	err := scanner.Scan(
		&article.SampleBatch,
		&article.SampleGroup,
		&article.ArticleID,
		&predictedPositive,
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
		&humanPositive,
		&article.ReviewerUsername,
		&article.ReviewNotes,
		&article.ReviewedAt,
		&article.UpdatedAt,
	)
	if err != nil {
		return AuditArticle{}, err
	}
	article.PredictedPositive = predictedPositive != 0
	article.ClassifierCaucasian = classifierCaucasian != 0
	article.ClassifierWhite = classifierWhite != 0
	article.ClassifierEuropean = classifierEuropean != 0
	article.ClassifierOther = classifierOther != 0
	if pubYear.Valid {
		article.PubYear = int(pubYear.Int64)
	}
	article.HumanPositive = parseNullableBool(humanPositive)
	return article, nil
}

func loadAuditArticle(db *sql.DB, batch string, articleID int) (AuditArticle, error) {
	row := db.QueryRow(`
		SELECT
			sample_batch,
			sample_group,
			article_id,
			predicted_positive,
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
			human_positive,
			COALESCE(reviewer_username, ''),
			COALESCE(review_notes, ''),
			COALESCE(reviewed_at, ''),
			COALESCE(updated_at, '')
		FROM audit_articles
		WHERE sample_batch = ? AND article_id = ?
	`, batch, articleID)
	return scanAuditArticle(row)
}

func firstPendingArticleID(db *sql.DB, batch, group string) (int, error) {
	var articleID int
	err := db.QueryRow(`
		SELECT article_id
		FROM audit_articles
		WHERE sample_batch = ?
		  AND sample_group = ?
		  AND human_positive IS NULL
		ORDER BY article_id
		LIMIT 1
	`, batch, group).Scan(&articleID)
	if err == sql.ErrNoRows {
		err = db.QueryRow(`
			SELECT article_id
			FROM audit_articles
			WHERE sample_batch = ?
			  AND sample_group = ?
			ORDER BY article_id
			LIMIT 1
		`, batch, group).Scan(&articleID)
	}
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return articleID, err
}

func adjacentArticleIDs(db *sql.DB, batch, group string, articleID int) (int, int, error) {
	var prevID int
	var nextID int
	err := db.QueryRow(`
		SELECT COALESCE(MAX(article_id), 0)
		FROM audit_articles
		WHERE sample_batch = ?
		  AND sample_group = ?
		  AND article_id < ?
	`, batch, group, articleID).Scan(&prevID)
	if err != nil {
		return 0, 0, err
	}
	err = db.QueryRow(`
		SELECT COALESCE(MIN(article_id), 0)
		FROM audit_articles
		WHERE sample_batch = ?
		  AND sample_group = ?
		  AND article_id > ?
	`, batch, group, articleID).Scan(&nextID)
	return prevID, nextID, err
}

func listAuditArticles(db *sql.DB, batch, group, status string) ([]ArticleRow, error) {
	query := `
		SELECT
			sample_batch,
			sample_group,
			article_id,
			predicted_positive,
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
			human_positive,
			COALESCE(reviewer_username, ''),
			COALESCE(review_notes, ''),
			COALESCE(reviewed_at, ''),
			COALESCE(updated_at, '')
		FROM audit_articles
		WHERE sample_batch = ?
	`
	args := []any{batch}
	if group != "" {
		query += " AND sample_group = ?"
		args = append(args, group)
	}
	if status == "pending" {
		query += " AND human_positive IS NULL"
	} else if status == "reviewed" {
		query += " AND human_positive IS NOT NULL"
	}
	query += " ORDER BY sample_group, article_id"

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
	return parts
}

func boolLabel(v bool) string {
	if v {
		return "positive"
	}
	return "negative"
}

func outcomeLabel(v string) string {
	if v == "" {
		return "pending"
	}
	return strings.ReplaceAll(v, "_", " ")
}

func queryWithBatch(batch string) string {
	return "?batch=" + url.QueryEscape(batch)
}

var templateFuncs = template.FuncMap{
	"boolLabel":       boolLabel,
	"outcomeLabel":    outcomeLabel,
	"formatTimestamp": formatTimestamp,
	"queryWithBatch":  queryWithBatch,
	"joinPhrases": func(article AuditArticle) string {
		return strings.Join(phraseList(article), ", ")
	},
	"articleOutcomeLabel": func(article AuditArticle) string {
		return outcomeLabel(articleOutcome(article))
	},
	"articleReviewStatus": func(article AuditArticle) string {
		return articleReviewStatus(article)
	},
	"yearLabel": func(year int) string {
		if year == 0 {
			return "—"
		}
		return fmt.Sprintf("%d", year)
	},
}
