package main

import (
	"database/sql"
	"fmt"
)

type HumanSubjectBatchMeta struct {
	BatchSlug            string
	Seed                 int
	SampleSize           int
	AIHumanSampleSize    int
	AINotHumanSampleSize int
	CreatedAt            string
	CreatedBy            string
	SourceFilter         string
	Notes                string
}

type HumanSubjectSummary struct {
	TotalCount            int
	ReviewedCount         int
	PendingCount          int
	AIHumanCount          int
	AINotHumanCount       int
	ReviewerHumanCount    int
	ReviewerNotHumanCount int
	CorrectCount          int
	FalsePositiveCount    int
	FalseNegativeCount    int
}

type HumanSubjectArticle struct {
	BatchSlug           string
	ClassificationID    int
	ArticleID           int64
	WorkID              int64
	WorkVersionID       int64
	DOI                 string
	JournalName         string
	PubYear             int
	Title               string
	Abstract            string
	AIAboutHumans       bool
	AIEvidence          string
	AIConfidence        string
	AIModel             string
	AIPromptTokens      int
	AICompletionTokens  int
	ReviewerAboutHumans *bool
	ReviewerUsername    string
	ReviewNotes         string
	ReviewedAt          string
	UpdatedAt           string
}

type HumanSubjectArticleRow struct {
	HumanSubjectArticle
	AuditOutcome string
	ReviewStatus string
}

func loadCurrentHumanSubjectBatch(db *sql.DB) (string, error) {
	var batch string
	err := db.QueryRow(`
		SELECT batch_slug
		FROM human_subject_audit_batches
		ORDER BY datetime(created_at) DESC, batch_slug DESC
		LIMIT 1
	`).Scan(&batch)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return batch, err
}

func loadHumanSubjectBatchMeta(db *sql.DB, batch string) (HumanSubjectBatchMeta, error) {
	var meta HumanSubjectBatchMeta
	err := db.QueryRow(`
		SELECT
			batch_slug,
			seed,
			sample_size,
			ai_human_sample_size,
			ai_not_human_sample_size,
			created_at,
			COALESCE(created_by, ''),
			COALESCE(source_filter, ''),
			COALESCE(notes, '')
		FROM human_subject_audit_batches
		WHERE batch_slug = ?
	`, batch).Scan(
		&meta.BatchSlug,
		&meta.Seed,
		&meta.SampleSize,
		&meta.AIHumanSampleSize,
		&meta.AINotHumanSampleSize,
		&meta.CreatedAt,
		&meta.CreatedBy,
		&meta.SourceFilter,
		&meta.Notes,
	)
	return meta, err
}

func loadHumanSubjectSummary(db *sql.DB, batch string) (HumanSubjectSummary, error) {
	var summary HumanSubjectSummary
	err := db.QueryRow(`
		SELECT
			COUNT(*) AS total_count,
			COALESCE(SUM(CASE WHEN reviewer_about_humans IS NOT NULL THEN 1 ELSE 0 END), 0) AS reviewed_count,
			COALESCE(SUM(CASE WHEN reviewer_about_humans IS NULL THEN 1 ELSE 0 END), 0) AS pending_count,
			COALESCE(SUM(CASE WHEN ai_about_humans = 1 THEN 1 ELSE 0 END), 0) AS ai_human_count,
			COALESCE(SUM(CASE WHEN ai_about_humans = 0 THEN 1 ELSE 0 END), 0) AS ai_not_human_count,
			COALESCE(SUM(CASE WHEN reviewer_about_humans = 1 THEN 1 ELSE 0 END), 0) AS reviewer_human_count,
			COALESCE(SUM(CASE WHEN reviewer_about_humans = 0 THEN 1 ELSE 0 END), 0) AS reviewer_not_human_count,
			COALESCE(SUM(CASE WHEN reviewer_about_humans IS NOT NULL AND reviewer_about_humans = ai_about_humans THEN 1 ELSE 0 END), 0) AS correct_count,
			COALESCE(SUM(CASE WHEN reviewer_about_humans = 0 AND ai_about_humans = 1 THEN 1 ELSE 0 END), 0) AS false_positive_count,
			COALESCE(SUM(CASE WHEN reviewer_about_humans = 1 AND ai_about_humans = 0 THEN 1 ELSE 0 END), 0) AS false_negative_count
		FROM human_subject_audit_articles
		WHERE batch_slug = ?
	`, batch).Scan(
		&summary.TotalCount,
		&summary.ReviewedCount,
		&summary.PendingCount,
		&summary.AIHumanCount,
		&summary.AINotHumanCount,
		&summary.ReviewerHumanCount,
		&summary.ReviewerNotHumanCount,
		&summary.CorrectCount,
		&summary.FalsePositiveCount,
		&summary.FalseNegativeCount,
	)
	return summary, err
}

func humanSubjectReviewStatus(article HumanSubjectArticle) string {
	if article.ReviewerAboutHumans == nil {
		return "pending"
	}
	return "reviewed"
}

func humanSubjectOutcome(article HumanSubjectArticle) string {
	if article.ReviewerAboutHumans == nil {
		return ""
	}
	if *article.ReviewerAboutHumans == article.AIAboutHumans {
		return "correct"
	}
	if article.AIAboutHumans {
		return "false positive"
	}
	return "false negative"
}

func scanHumanSubjectArticle(scanner interface {
	Scan(dest ...any) error
}) (HumanSubjectArticle, error) {
	var article HumanSubjectArticle
	var articleID sql.NullInt64
	var workID sql.NullInt64
	var workVersionID sql.NullInt64
	var pubYear sql.NullInt64
	var aiAboutHumans int
	var reviewerAboutHumans sql.NullInt64
	var aiPromptTokens sql.NullInt64
	var aiCompletionTokens sql.NullInt64
	err := scanner.Scan(
		&article.BatchSlug,
		&article.ClassificationID,
		&articleID,
		&workID,
		&workVersionID,
		&article.DOI,
		&article.JournalName,
		&pubYear,
		&article.Title,
		&article.Abstract,
		&aiAboutHumans,
		&article.AIEvidence,
		&article.AIConfidence,
		&article.AIModel,
		&aiPromptTokens,
		&aiCompletionTokens,
		&reviewerAboutHumans,
		&article.ReviewerUsername,
		&article.ReviewNotes,
		&article.ReviewedAt,
		&article.UpdatedAt,
	)
	if err != nil {
		return HumanSubjectArticle{}, err
	}
	if articleID.Valid {
		article.ArticleID = articleID.Int64
	}
	if workID.Valid {
		article.WorkID = workID.Int64
	}
	if workVersionID.Valid {
		article.WorkVersionID = workVersionID.Int64
	}
	if pubYear.Valid {
		article.PubYear = int(pubYear.Int64)
	}
	article.AIAboutHumans = aiAboutHumans != 0
	if aiPromptTokens.Valid {
		article.AIPromptTokens = int(aiPromptTokens.Int64)
	}
	if aiCompletionTokens.Valid {
		article.AICompletionTokens = int(aiCompletionTokens.Int64)
	}
	article.ReviewerAboutHumans = parseNullableBool(reviewerAboutHumans)
	return article, nil
}

func humanSubjectArticleSelectSQL() string {
	return `
		SELECT
			batch_slug,
			classification_id,
			article_id,
			work_id,
			work_version_id,
			COALESCE(doi, ''),
			COALESCE(journal_name, ''),
			pub_year,
			COALESCE(title, ''),
			COALESCE(abstract, ''),
			ai_about_humans,
			COALESCE(ai_evidence, ''),
			COALESCE(ai_confidence, ''),
			COALESCE(ai_model, ''),
			ai_prompt_tokens,
			ai_completion_tokens,
			reviewer_about_humans,
			COALESCE(reviewer_username, ''),
			COALESCE(review_notes, ''),
			COALESCE(reviewed_at, ''),
			COALESCE(updated_at, '')
		FROM human_subject_audit_articles
	`
}

func loadHumanSubjectArticle(db *sql.DB, batch string, classificationID int) (HumanSubjectArticle, error) {
	row := db.QueryRow(humanSubjectArticleSelectSQL()+`
		WHERE batch_slug = ? AND classification_id = ?
	`, batch, classificationID)
	return scanHumanSubjectArticle(row)
}

func firstPendingHumanSubjectClassificationID(db *sql.DB, batch string) (int, error) {
	var classificationID int
	err := db.QueryRow(`
		SELECT classification_id
		FROM human_subject_audit_articles
		WHERE batch_slug = ?
		  AND reviewer_about_humans IS NULL
		ORDER BY ai_about_humans DESC, classification_id
		LIMIT 1
	`, batch).Scan(&classificationID)
	if err == sql.ErrNoRows {
		err = db.QueryRow(`
			SELECT classification_id
			FROM human_subject_audit_articles
			WHERE batch_slug = ?
			ORDER BY ai_about_humans DESC, classification_id
			LIMIT 1
		`, batch).Scan(&classificationID)
	}
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return classificationID, err
}

func adjacentHumanSubjectClassificationIDs(db *sql.DB, batch string, classificationID int) (int, int, error) {
	var prevID int
	var nextID int
	err := db.QueryRow(`
		SELECT COALESCE(MAX(classification_id), 0)
		FROM human_subject_audit_articles
		WHERE batch_slug = ?
		  AND classification_id < ?
	`, batch, classificationID).Scan(&prevID)
	if err != nil {
		return 0, 0, err
	}
	err = db.QueryRow(`
		SELECT COALESCE(MIN(classification_id), 0)
		FROM human_subject_audit_articles
		WHERE batch_slug = ?
		  AND classification_id > ?
	`, batch, classificationID).Scan(&nextID)
	return prevID, nextID, err
}

func listHumanSubjectArticles(db *sql.DB, batch, reviewStatus, aiDecision string) ([]HumanSubjectArticleRow, error) {
	query := humanSubjectArticleSelectSQL() + `
		WHERE batch_slug = ?
	`
	args := []any{batch}
	if reviewStatus == "pending" {
		query += " AND reviewer_about_humans IS NULL"
	} else if reviewStatus == "reviewed" {
		query += " AND reviewer_about_humans IS NOT NULL"
	}
	if aiDecision == "human" {
		query += " AND ai_about_humans = 1"
	} else if aiDecision == "not_human" {
		query += " AND ai_about_humans = 0"
	}
	query += " ORDER BY ai_about_humans DESC, classification_id"

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []HumanSubjectArticleRow
	for rows.Next() {
		article, err := scanHumanSubjectArticle(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, HumanSubjectArticleRow{
			HumanSubjectArticle: article,
			AuditOutcome:        humanSubjectOutcome(article),
			ReviewStatus:        humanSubjectReviewStatus(article),
		})
	}
	return out, rows.Err()
}

func humanSubjectBoolLabel(value bool) string {
	if value {
		return "about humans"
	}
	return "not about humans"
}

func humanSubjectReviewerLabel(value *bool) string {
	if value == nil {
		return "pending"
	}
	return humanSubjectBoolLabel(*value)
}

func humanSubjectSummaryLabel(summary HumanSubjectSummary) string {
	return fmt.Sprintf(
		"%d reviewed / %d total; %d correct, %d false positive, %d false negative",
		summary.ReviewedCount,
		summary.TotalCount,
		summary.CorrectCount,
		summary.FalsePositiveCount,
		summary.FalseNegativeCount,
	)
}
