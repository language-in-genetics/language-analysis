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
