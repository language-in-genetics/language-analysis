ALTER TABLE languageingenetics.batches
ADD COLUMN IF NOT EXISTS batch_kind TEXT NOT NULL DEFAULT 'term_analysis';

UPDATE languageingenetics.batches
SET batch_kind = 'term_analysis'
WHERE batch_kind IS NULL;

CREATE TABLE IF NOT EXISTS languageingenetics.human_subject_classifications (
    id BIGSERIAL PRIMARY KEY,
    article_id BIGINT,
    work_id BIGINT,
    work_version_id BIGINT,
    has_abstract BOOLEAN,
    pub_year INTEGER,
    processed BOOLEAN NOT NULL DEFAULT false,
    batch_id INTEGER REFERENCES languageingenetics.batches(id),
    when_processed TIMESTAMPTZ,
    about_humans BOOLEAN,
    human_evidence TEXT,
    confidence TEXT,
    model TEXT,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    classification_version TEXT NOT NULL DEFAULT 'human-subject-title-abstract-v1',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS human_subject_classifications_work_version_uidx
    ON languageingenetics.human_subject_classifications (work_version_id)
    WHERE work_version_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS human_subject_classifications_article_uidx
    ON languageingenetics.human_subject_classifications (article_id)
    WHERE article_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS human_subject_classifications_batch_idx
    ON languageingenetics.human_subject_classifications (batch_id);

CREATE INDEX IF NOT EXISTS human_subject_classifications_processed_work_version_idx
    ON languageingenetics.human_subject_classifications (processed, work_version_id)
    WHERE work_version_id IS NOT NULL;
