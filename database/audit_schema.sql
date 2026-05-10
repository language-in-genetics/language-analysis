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

CREATE TABLE IF NOT EXISTS languageingenetics.fulltext_audit_batches (
    id BIGSERIAL PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    seed INTEGER NOT NULL,
    sample_size INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    source_filter TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS languageingenetics.fulltext_audit_articles (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL REFERENCES languageingenetics.fulltext_audit_batches(id) ON DELETE CASCADE,
    article_id INTEGER NOT NULL,
    work_id BIGINT,
    work_version_id BIGINT,
    doi TEXT,
    journal_name TEXT,
    pub_year INTEGER,
    title TEXT,
    abstract TEXT,
    fulltext_status TEXT NOT NULL DEFAULT 'pending_fetch' CHECK (
        fulltext_status IN (
            'pending_fetch',
            'available',
            'needs_manual',
            'unavailable',
            'extraction_failed'
        )
    ),
    fulltext_source TEXT,
    fulltext_path TEXT,
    extracted_text TEXT,
    ai_analysis_status TEXT NOT NULL DEFAULT 'not_queued' CHECK (
        ai_analysis_status IN (
            'not_queued',
            'queued',
            'processed',
            'failed'
        )
    ),
    ai_caucasian BOOLEAN,
    ai_white BOOLEAN,
    ai_european BOOLEAN,
    ai_european_phrase_used TEXT,
    ai_other BOOLEAN,
    ai_other_phrase_used TEXT,
    ai_model TEXT,
    ai_prompt_tokens INTEGER,
    ai_completion_tokens INTEGER,
    ai_error TEXT,
    ai_processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (batch_id, article_id)
);

CREATE INDEX IF NOT EXISTS fulltext_audit_articles_batch_status_idx
    ON languageingenetics.fulltext_audit_articles (batch_id, fulltext_status, article_id);

CREATE INDEX IF NOT EXISTS fulltext_audit_articles_article_idx
    ON languageingenetics.fulltext_audit_articles (article_id);

ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_analysis_status TEXT NOT NULL DEFAULT 'not_queued';
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_caucasian BOOLEAN;
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_white BOOLEAN;
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_european BOOLEAN;
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_european_phrase_used TEXT;
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_other BOOLEAN;
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_other_phrase_used TEXT;
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_model TEXT;
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_prompt_tokens INTEGER;
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_completion_tokens INTEGER;
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_error TEXT;
ALTER TABLE languageingenetics.fulltext_audit_articles
    ADD COLUMN IF NOT EXISTS ai_processed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS fulltext_audit_articles_ai_status_idx
    ON languageingenetics.fulltext_audit_articles (ai_analysis_status, batch_id, article_id);

CREATE TABLE IF NOT EXISTS languageingenetics.fulltext_audit_reviews (
    sample_article_id BIGINT PRIMARY KEY REFERENCES languageingenetics.fulltext_audit_articles(id) ON DELETE CASCADE,
    terminology_present BOOLEAN,
    caucasian_present BOOLEAN,
    white_present BOOLEAN,
    european_present BOOLEAN,
    other_present BOOLEAN,
    quoted_evidence TEXT,
    reviewer_username TEXT,
    review_notes TEXT,
    reviewed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source TEXT NOT NULL DEFAULT 'merah_audit_sqlite'
);

DROP VIEW IF EXISTS languageingenetics.fulltext_audit_status_view;

CREATE OR REPLACE VIEW languageingenetics.fulltext_audit_status_view AS
SELECT
    b.id AS batch_id,
    b.slug AS sample_batch,
    b.seed,
    b.sample_size,
    b.created_at AS batch_created_at,
    b.created_by,
    b.source_filter,
    b.notes,
    s.id AS sample_article_id,
    s.article_id,
    s.work_id,
    s.work_version_id,
    s.doi,
    s.journal_name,
    s.pub_year,
    s.title,
    s.abstract,
    s.fulltext_status,
    s.fulltext_source,
    s.fulltext_path,
    s.extracted_text,
    s.ai_analysis_status,
    s.ai_caucasian,
    s.ai_white,
    s.ai_european,
    s.ai_european_phrase_used,
    s.ai_other,
    s.ai_other_phrase_used,
    s.ai_model,
    s.ai_prompt_tokens,
    s.ai_completion_tokens,
    s.ai_error,
    s.ai_processed_at,
    r.terminology_present,
    r.caucasian_present,
    r.white_present,
    r.european_present,
    r.other_present,
    r.quoted_evidence,
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
        WHEN r.terminology_present THEN 'tracked_terminology_present'
        ELSE 'no_tracked_terminology'
    END AS audit_outcome
FROM languageingenetics.fulltext_audit_batches b
JOIN languageingenetics.fulltext_audit_articles s
    ON s.batch_id = b.id
LEFT JOIN languageingenetics.fulltext_audit_reviews r
    ON r.sample_article_id = s.id;
