package main

import (
	"database/sql"
	"fmt"
	"strings"
)

const fulltextStatusOrderSQL = `CASE fulltext_status
	WHEN 'available' THEN 0
	WHEN 'needs_manual' THEN 1
	WHEN 'pending_fetch' THEN 2
	WHEN 'extraction_failed' THEN 3
	WHEN 'unavailable' THEN 4
	ELSE 99
END`

type FulltextBatchMeta struct {
	BatchSlug    string
	Seed         int
	SampleSize   int
	CreatedAt    string
	CreatedBy    string
	SourceFilter string
	Notes        string
}

type FulltextSummary struct {
	TotalCount            int
	ReviewedCount         int
	PendingCount          int
	TerminologyCount      int
	NoTerminologyCount    int
	AvailableCount        int
	NeedsManualCount      int
	PendingFetchCount     int
	UnavailableCount      int
	ExtractionFailedCount int
}

type FulltextArticle struct {
	BatchSlug          string
	ArticleID          int
	WorkID             int64
	WorkVersionID      int64
	DOI                string
	JournalName        string
	PubYear            int
	Title              string
	Abstract           string
	FulltextStatus     string
	FulltextSource     string
	FulltextPath       string
	ExtractedText      string
	AIAnalysisStatus   string
	AICaucasian        bool
	AIWhite            bool
	AIEuropean         bool
	AIEuropeanPhrase   string
	AIOther            bool
	AIOtherPhrase      string
	AIModel            string
	AIPromptTokens     int
	AICompletionTokens int
	AIError            string
	AIProcessedAt      string
	TerminologyPresent *bool
	CaucasianPresent   bool
	WhitePresent       bool
	EuropeanPresent    bool
	OtherPresent       bool
	QuotedEvidence     string
	ReviewerUsername   string
	ReviewNotes        string
	ReviewedAt         string
	UpdatedAt          string
}

type FulltextArticleRow struct {
	FulltextArticle
	AuditOutcome string
	ReviewStatus string
}

func loadCurrentFulltextBatch(db *sql.DB) (string, error) {
	var batch string
	err := db.QueryRow(`
		SELECT batch_slug
		FROM fulltext_batches
		ORDER BY datetime(created_at) DESC, batch_slug DESC
		LIMIT 1
	`).Scan(&batch)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return batch, err
}

func loadFulltextBatchMeta(db *sql.DB, batch string) (FulltextBatchMeta, error) {
	var meta FulltextBatchMeta
	err := db.QueryRow(`
		SELECT
			batch_slug,
			seed,
			sample_size,
			created_at,
			COALESCE(created_by, ''),
			COALESCE(source_filter, ''),
			COALESCE(notes, '')
		FROM fulltext_batches
		WHERE batch_slug = ?
	`, batch).Scan(
		&meta.BatchSlug,
		&meta.Seed,
		&meta.SampleSize,
		&meta.CreatedAt,
		&meta.CreatedBy,
		&meta.SourceFilter,
		&meta.Notes,
	)
	return meta, err
}

func loadFulltextSummary(db *sql.DB, batch string) (FulltextSummary, error) {
	var summary FulltextSummary
	err := db.QueryRow(`
		SELECT
			COUNT(*) AS total_count,
			COALESCE(SUM(CASE WHEN terminology_present IS NOT NULL THEN 1 ELSE 0 END), 0) AS reviewed_count,
			COALESCE(SUM(CASE WHEN terminology_present IS NULL THEN 1 ELSE 0 END), 0) AS pending_count,
			COALESCE(SUM(CASE WHEN terminology_present = 1 THEN 1 ELSE 0 END), 0) AS terminology_count,
			COALESCE(SUM(CASE WHEN terminology_present = 0 THEN 1 ELSE 0 END), 0) AS no_terminology_count,
			COALESCE(SUM(CASE WHEN fulltext_status = 'available' THEN 1 ELSE 0 END), 0) AS available_count,
			COALESCE(SUM(CASE WHEN fulltext_status = 'needs_manual' THEN 1 ELSE 0 END), 0) AS needs_manual_count,
			COALESCE(SUM(CASE WHEN fulltext_status = 'pending_fetch' THEN 1 ELSE 0 END), 0) AS pending_fetch_count,
			COALESCE(SUM(CASE WHEN fulltext_status = 'unavailable' THEN 1 ELSE 0 END), 0) AS unavailable_count,
			COALESCE(SUM(CASE WHEN fulltext_status = 'extraction_failed' THEN 1 ELSE 0 END), 0) AS extraction_failed_count
		FROM fulltext_articles
		WHERE batch_slug = ?
	`, batch).Scan(
		&summary.TotalCount,
		&summary.ReviewedCount,
		&summary.PendingCount,
		&summary.TerminologyCount,
		&summary.NoTerminologyCount,
		&summary.AvailableCount,
		&summary.NeedsManualCount,
		&summary.PendingFetchCount,
		&summary.UnavailableCount,
		&summary.ExtractionFailedCount,
	)
	return summary, err
}

func fulltextReviewStatus(article FulltextArticle) string {
	if article.TerminologyPresent == nil {
		return "pending"
	}
	return "reviewed"
}

func fulltextOutcome(article FulltextArticle) string {
	if article.TerminologyPresent == nil {
		return ""
	}
	if *article.TerminologyPresent {
		return "tracked terminology present"
	}
	return "no tracked terminology"
}

func scanFulltextArticle(scanner interface {
	Scan(dest ...any) error
}) (FulltextArticle, error) {
	var article FulltextArticle
	var pubYear sql.NullInt64
	var workID sql.NullInt64
	var workVersionID sql.NullInt64
	var terminologyPresent sql.NullInt64
	var caucasianPresent sql.NullInt64
	var whitePresent sql.NullInt64
	var europeanPresent sql.NullInt64
	var otherPresent sql.NullInt64
	var aiCaucasian sql.NullInt64
	var aiWhite sql.NullInt64
	var aiEuropean sql.NullInt64
	var aiOther sql.NullInt64
	var aiPromptTokens sql.NullInt64
	var aiCompletionTokens sql.NullInt64
	err := scanner.Scan(
		&article.BatchSlug,
		&article.ArticleID,
		&workID,
		&workVersionID,
		&article.DOI,
		&article.JournalName,
		&pubYear,
		&article.Title,
		&article.Abstract,
		&article.FulltextStatus,
		&article.FulltextSource,
		&article.FulltextPath,
		&article.ExtractedText,
		&article.AIAnalysisStatus,
		&aiCaucasian,
		&aiWhite,
		&aiEuropean,
		&article.AIEuropeanPhrase,
		&aiOther,
		&article.AIOtherPhrase,
		&article.AIModel,
		&aiPromptTokens,
		&aiCompletionTokens,
		&article.AIError,
		&article.AIProcessedAt,
		&terminologyPresent,
		&caucasianPresent,
		&whitePresent,
		&europeanPresent,
		&otherPresent,
		&article.QuotedEvidence,
		&article.ReviewerUsername,
		&article.ReviewNotes,
		&article.ReviewedAt,
		&article.UpdatedAt,
	)
	if err != nil {
		return FulltextArticle{}, err
	}
	if pubYear.Valid {
		article.PubYear = int(pubYear.Int64)
	}
	if workID.Valid {
		article.WorkID = workID.Int64
	}
	if workVersionID.Valid {
		article.WorkVersionID = workVersionID.Int64
	}
	article.TerminologyPresent = parseNullableBool(terminologyPresent)
	article.CaucasianPresent = caucasianPresent.Valid && caucasianPresent.Int64 != 0
	article.WhitePresent = whitePresent.Valid && whitePresent.Int64 != 0
	article.EuropeanPresent = europeanPresent.Valid && europeanPresent.Int64 != 0
	article.OtherPresent = otherPresent.Valid && otherPresent.Int64 != 0
	article.AICaucasian = aiCaucasian.Valid && aiCaucasian.Int64 != 0
	article.AIWhite = aiWhite.Valid && aiWhite.Int64 != 0
	article.AIEuropean = aiEuropean.Valid && aiEuropean.Int64 != 0
	article.AIOther = aiOther.Valid && aiOther.Int64 != 0
	if aiPromptTokens.Valid {
		article.AIPromptTokens = int(aiPromptTokens.Int64)
	}
	if aiCompletionTokens.Valid {
		article.AICompletionTokens = int(aiCompletionTokens.Int64)
	}
	return article, nil
}

func fulltextArticleSelectSQL() string {
	return `
		SELECT
			batch_slug,
			article_id,
			work_id,
			work_version_id,
			COALESCE(doi, ''),
			COALESCE(journal_name, ''),
			pub_year,
			COALESCE(title, ''),
			COALESCE(abstract, ''),
			fulltext_status,
			COALESCE(fulltext_source, ''),
			COALESCE(fulltext_path, ''),
			COALESCE(extracted_text, ''),
			COALESCE(ai_analysis_status, 'not_queued'),
			ai_caucasian,
			ai_white,
			ai_european,
			COALESCE(ai_european_phrase_used, ''),
			ai_other,
			COALESCE(ai_other_phrase_used, ''),
			COALESCE(ai_model, ''),
			ai_prompt_tokens,
			ai_completion_tokens,
			COALESCE(ai_error, ''),
			COALESCE(ai_processed_at, ''),
			terminology_present,
			caucasian_present,
			white_present,
			european_present,
			other_present,
			COALESCE(quoted_evidence, ''),
			COALESCE(reviewer_username, ''),
			COALESCE(review_notes, ''),
			COALESCE(reviewed_at, ''),
			COALESCE(updated_at, '')
		FROM fulltext_articles
	`
}

func loadFulltextArticle(db *sql.DB, batch string, articleID int) (FulltextArticle, error) {
	row := db.QueryRow(fulltextArticleSelectSQL()+`
		WHERE batch_slug = ? AND article_id = ?
	`, batch, articleID)
	return scanFulltextArticle(row)
}

func firstPendingFulltextArticleID(db *sql.DB, batch string) (int, error) {
	var articleID int
	err := db.QueryRow(`
		SELECT article_id
		FROM fulltext_articles
		WHERE batch_slug = ?
		  AND terminology_present IS NULL
		ORDER BY `+fulltextStatusOrderSQL+`, article_id
		LIMIT 1
	`, batch).Scan(&articleID)
	if err == sql.ErrNoRows {
		err = db.QueryRow(`
			SELECT article_id
			FROM fulltext_articles
			WHERE batch_slug = ?
			ORDER BY `+fulltextStatusOrderSQL+`, article_id
			LIMIT 1
		`, batch).Scan(&articleID)
	}
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return articleID, err
}

func adjacentFulltextArticleIDs(db *sql.DB, batch string, articleID int) (int, int, error) {
	var prevID int
	var nextID int
	err := db.QueryRow(`
		SELECT COALESCE(MAX(article_id), 0)
		FROM fulltext_articles
		WHERE batch_slug = ?
		  AND article_id < ?
	`, batch, articleID).Scan(&prevID)
	if err != nil {
		return 0, 0, err
	}
	err = db.QueryRow(`
		SELECT COALESCE(MIN(article_id), 0)
		FROM fulltext_articles
		WHERE batch_slug = ?
		  AND article_id > ?
	`, batch, articleID).Scan(&nextID)
	return prevID, nextID, err
}

func listFulltextArticles(db *sql.DB, batch, reviewStatus, fulltextStatus string) ([]FulltextArticleRow, error) {
	query := fulltextArticleSelectSQL() + `
		WHERE batch_slug = ?
	`
	args := []any{batch}
	if reviewStatus == "pending" {
		query += " AND terminology_present IS NULL"
	} else if reviewStatus == "reviewed" {
		query += " AND terminology_present IS NOT NULL"
	}
	if fulltextStatus != "" {
		query += " AND fulltext_status = ?"
		args = append(args, fulltextStatus)
	}
	query += " ORDER BY " + fulltextStatusOrderSQL + ", article_id"

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FulltextArticleRow
	for rows.Next() {
		article, err := scanFulltextArticle(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, FulltextArticleRow{
			FulltextArticle: article,
			AuditOutcome:    fulltextOutcome(article),
			ReviewStatus:    fulltextReviewStatus(article),
		})
	}
	return out, rows.Err()
}

func fulltextStatusDisplay(status string) string {
	switch status {
	case "pending_fetch":
		return "pending fetch"
	case "available":
		return "available"
	case "needs_manual":
		return "needs manual"
	case "unavailable":
		return "unavailable"
	case "extraction_failed":
		return "extraction failed"
	default:
		return status
	}
}

func fulltextTermList(article FulltextArticle) string {
	terms := []string{}
	if article.CaucasianPresent {
		terms = append(terms, "caucasian")
	}
	if article.WhitePresent {
		terms = append(terms, "white")
	}
	if article.EuropeanPresent {
		terms = append(terms, "european")
	}
	if article.OtherPresent {
		terms = append(terms, "other")
	}
	if len(terms) == 0 {
		return "none marked"
	}
	return strings.Join(terms, ", ")
}

func fulltextAITermList(article FulltextArticle) string {
	terms := []string{}
	if article.AICaucasian {
		terms = append(terms, "caucasian")
	}
	if article.AIWhite {
		terms = append(terms, "white")
	}
	if article.AIEuropean {
		label := "european"
		if article.AIEuropeanPhrase != "" {
			label += ": " + article.AIEuropeanPhrase
		}
		terms = append(terms, label)
	}
	if article.AIOther {
		label := "other"
		if article.AIOtherPhrase != "" {
			label += ": " + article.AIOtherPhrase
		}
		terms = append(terms, label)
	}
	if len(terms) == 0 {
		return "none flagged"
	}
	return strings.Join(terms, ", ")
}

func fulltextSummaryLabel(summary FulltextSummary) string {
	return fmt.Sprintf(
		"%d reviewed / %d total; %d tracked terminology, %d no tracked terminology",
		summary.ReviewedCount,
		summary.TotalCount,
		summary.TerminologyCount,
		summary.NoTerminologyCount,
	)
}
