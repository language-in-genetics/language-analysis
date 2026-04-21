CREATE TABLE IF NOT EXISTS languageingenetics.audit_sample_batches (
    id BIGSERIAL PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    seed INTEGER NOT NULL,
    matched_label_sample_size INTEGER NOT NULL,
    none_of_these_labels_sample_size INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    source_filter TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS languageingenetics.audit_sample_articles (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL REFERENCES languageingenetics.audit_sample_batches(id) ON DELETE CASCADE,
    article_id INTEGER NOT NULL,
    target_label TEXT NOT NULL CHECK (
        target_label IN (
            'caucasian',
            'white',
            'european',
            'other',
            'none_of_these_labels'
        )
    ),
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
    UNIQUE (batch_id, target_label, article_id)
);

CREATE INDEX IF NOT EXISTS audit_sample_articles_batch_target_label_idx
    ON languageingenetics.audit_sample_articles (batch_id, target_label, article_id);

CREATE INDEX IF NOT EXISTS audit_sample_articles_article_idx
    ON languageingenetics.audit_sample_articles (article_id);

CREATE TABLE IF NOT EXISTS languageingenetics.audit_article_reviews (
    sample_article_id BIGINT PRIMARY KEY REFERENCES languageingenetics.audit_sample_articles(id) ON DELETE CASCADE,
    target_confirmed BOOLEAN,
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
    b.matched_label_sample_size,
    b.none_of_these_labels_sample_size,
    b.created_at AS batch_created_at,
    s.id AS sample_article_id,
    s.article_id,
    s.target_label,
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
    r.target_confirmed,
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
        WHEN r.target_confirmed THEN 'confirmed'
        ELSE 'disagreed'
    END AS audit_outcome
FROM languageingenetics.audit_sample_batches b
JOIN languageingenetics.audit_sample_articles s
    ON s.batch_id = b.id
LEFT JOIN languageingenetics.audit_article_reviews r
    ON r.sample_article_id = s.id;
