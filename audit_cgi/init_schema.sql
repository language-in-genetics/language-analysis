PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS audit_batches (
    sample_batch TEXT PRIMARY KEY,
    seed INTEGER NOT NULL,
    matched_label_sample_size INTEGER NOT NULL,
    none_of_these_labels_sample_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT,
    source_filter TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS audit_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_batch TEXT NOT NULL REFERENCES audit_batches(sample_batch) ON DELETE CASCADE,
    target_label TEXT NOT NULL CHECK (
        target_label IN (
            'caucasian',
            'white',
            'european',
            'other',
            'none_of_these_labels'
        )
    ),
    article_id INTEGER NOT NULL,
    doi TEXT,
    journal_name TEXT,
    pub_year INTEGER,
    title TEXT,
    abstract TEXT,
    classifier_caucasian INTEGER NOT NULL DEFAULT 0,
    classifier_white INTEGER NOT NULL DEFAULT 0,
    classifier_european INTEGER NOT NULL DEFAULT 0,
    classifier_other INTEGER NOT NULL DEFAULT 0,
    classifier_european_phrase_used TEXT,
    classifier_other_phrase_used TEXT,
    target_confirmed INTEGER CHECK (target_confirmed IN (0, 1)),
    reviewer_username TEXT,
    review_notes TEXT,
    reviewed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sample_batch, target_label, article_id)
);

CREATE INDEX IF NOT EXISTS audit_articles_batch_target_label_idx
    ON audit_articles (sample_batch, target_label, article_id);

CREATE INDEX IF NOT EXISTS audit_articles_reviewed_idx
    ON audit_articles (sample_batch, target_confirmed, reviewer_username);

CREATE TABLE IF NOT EXISTS fulltext_batches (
    batch_slug TEXT PRIMARY KEY,
    seed INTEGER NOT NULL,
    sample_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT,
    source_filter TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS fulltext_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_slug TEXT NOT NULL REFERENCES fulltext_batches(batch_slug) ON DELETE CASCADE,
    article_id INTEGER NOT NULL,
    work_id INTEGER,
    work_version_id INTEGER,
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
    uploaded_filename TEXT,
    uploaded_content_type TEXT,
    uploaded_size INTEGER,
    uploaded_blob BLOB,
    uploaded_at TEXT,
    extracted_text TEXT,
    ai_analysis_status TEXT NOT NULL DEFAULT 'not_queued' CHECK (
        ai_analysis_status IN (
            'not_queued',
            'queued',
            'processed',
            'failed'
        )
    ),
    ai_caucasian INTEGER CHECK (ai_caucasian IN (0, 1)),
    ai_white INTEGER CHECK (ai_white IN (0, 1)),
    ai_european INTEGER CHECK (ai_european IN (0, 1)),
    ai_european_phrase_used TEXT,
    ai_other INTEGER CHECK (ai_other IN (0, 1)),
    ai_other_phrase_used TEXT,
    ai_model TEXT,
    ai_prompt_tokens INTEGER,
    ai_completion_tokens INTEGER,
    ai_error TEXT,
    ai_processed_at TEXT,
    terminology_present INTEGER CHECK (terminology_present IN (0, 1)),
    caucasian_present INTEGER CHECK (caucasian_present IN (0, 1)),
    white_present INTEGER CHECK (white_present IN (0, 1)),
    european_present INTEGER CHECK (european_present IN (0, 1)),
    other_present INTEGER CHECK (other_present IN (0, 1)),
    quoted_evidence TEXT,
    reviewer_username TEXT,
    review_notes TEXT,
    reviewed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (batch_slug, article_id)
);

CREATE INDEX IF NOT EXISTS fulltext_articles_batch_reviewed_idx
    ON fulltext_articles (batch_slug, terminology_present, reviewer_username);

CREATE INDEX IF NOT EXISTS fulltext_articles_batch_status_idx
    ON fulltext_articles (batch_slug, fulltext_status, article_id);

CREATE TABLE IF NOT EXISTS human_subject_audit_batches (
    batch_slug TEXT PRIMARY KEY,
    seed INTEGER NOT NULL,
    sample_size INTEGER NOT NULL,
    ai_human_sample_size INTEGER NOT NULL,
    ai_not_human_sample_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT,
    source_filter TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS human_subject_audit_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_slug TEXT NOT NULL REFERENCES human_subject_audit_batches(batch_slug) ON DELETE CASCADE,
    classification_id INTEGER NOT NULL,
    article_id INTEGER,
    work_id INTEGER,
    work_version_id INTEGER,
    doi TEXT,
    journal_name TEXT,
    pub_year INTEGER,
    title TEXT,
    abstract TEXT,
    ai_about_humans INTEGER NOT NULL CHECK (ai_about_humans IN (0, 1)),
    ai_evidence TEXT,
    ai_confidence TEXT,
    ai_model TEXT,
    ai_prompt_tokens INTEGER,
    ai_completion_tokens INTEGER,
    reviewer_about_humans INTEGER CHECK (reviewer_about_humans IN (0, 1)),
    reviewer_username TEXT,
    review_notes TEXT,
    reviewed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (batch_slug, classification_id)
);

CREATE INDEX IF NOT EXISTS human_subject_audit_articles_batch_reviewed_idx
    ON human_subject_audit_articles (batch_slug, reviewer_about_humans, reviewer_username);

CREATE INDEX IF NOT EXISTS human_subject_audit_articles_batch_ai_idx
    ON human_subject_audit_articles (batch_slug, ai_about_humans, classification_id);
