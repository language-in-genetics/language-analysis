CREATE TABLE IF NOT EXISTS languageingenetics.audit_sample_batches (
    id BIGSERIAL PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    seed INTEGER NOT NULL,
    positive_sample_size INTEGER NOT NULL,
    negative_sample_size INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    source_filter TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS languageingenetics.audit_sample_articles (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL REFERENCES languageingenetics.audit_sample_batches(id) ON DELETE CASCADE,
    article_id INTEGER NOT NULL,
    sample_group TEXT NOT NULL CHECK (sample_group IN ('positive', 'negative')),
    predicted_positive BOOLEAN NOT NULL,
    doi TEXT,
    journal_name TEXT,
    pub_year INTEGER,
    title TEXT,
    abstract TEXT,
    classifier_caucasian BOOLEAN NOT NULL DEFAULT FALSE,
    classifier_white BOOLEAN NOT NULL DEFAULT FALSE,
    classifier_european BOOLEAN NOT NULL DEFAULT FALSE,
    classifier_other BOOLEAN NOT NULL DEFAULT FALSE,
    classifier_european_phrase_used TEXT,
    classifier_other_phrase_used TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (batch_id, article_id)
);

CREATE INDEX IF NOT EXISTS audit_sample_articles_batch_group_idx
    ON languageingenetics.audit_sample_articles (batch_id, sample_group, article_id);

CREATE INDEX IF NOT EXISTS audit_sample_articles_article_idx
    ON languageingenetics.audit_sample_articles (article_id);

CREATE TABLE IF NOT EXISTS languageingenetics.audit_article_reviews (
    sample_article_id BIGINT PRIMARY KEY REFERENCES languageingenetics.audit_sample_articles(id) ON DELETE CASCADE,
    human_positive BOOLEAN,
    reviewer_username TEXT,
    review_notes TEXT,
    reviewed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source TEXT NOT NULL DEFAULT 'merah_audit_sqlite'
);

CREATE OR REPLACE VIEW languageingenetics.audit_article_status_view AS
SELECT
    b.id AS batch_id,
    b.slug AS sample_batch,
    b.seed,
    b.positive_sample_size,
    b.negative_sample_size,
    b.created_at AS batch_created_at,
    s.id AS sample_article_id,
    s.article_id,
    s.sample_group,
    s.predicted_positive,
    s.doi,
    s.journal_name,
    s.pub_year,
    s.title,
    s.abstract,
    s.classifier_caucasian,
    s.classifier_white,
    s.classifier_european,
    s.classifier_other,
    s.classifier_european_phrase_used,
    s.classifier_other_phrase_used,
    r.human_positive,
    r.reviewer_username,
    r.review_notes,
    r.reviewed_at,
    r.updated_at AS review_updated_at,
    CASE
        WHEN r.sample_article_id IS NULL THEN 'pending'
        ELSE 'reviewed'
    END AS review_status,
    CASE
        WHEN r.sample_article_id IS NULL THEN NULL
        WHEN s.predicted_positive AND r.human_positive THEN 'true_positive'
        WHEN s.predicted_positive AND NOT r.human_positive THEN 'false_positive'
        WHEN NOT s.predicted_positive AND r.human_positive THEN 'false_negative'
        WHEN NOT s.predicted_positive AND NOT r.human_positive THEN 'true_negative'
        ELSE NULL
    END AS audit_outcome
FROM languageingenetics.audit_sample_batches b
JOIN languageingenetics.audit_sample_articles s
    ON s.batch_id = b.id
LEFT JOIN languageingenetics.audit_article_reviews r
    ON r.sample_article_id = s.id;
