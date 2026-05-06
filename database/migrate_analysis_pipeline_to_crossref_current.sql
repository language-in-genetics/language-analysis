-- Move the OpenAI analysis pipeline onto the canonical Crossref version tables.
--
-- This keeps legacy raw_text_data article IDs for old batches, but gives every
-- analysis row a stable Crossref work/version reference for new annual imports.

ALTER TABLE languageingenetics.files
    ADD COLUMN IF NOT EXISTS work_id BIGINT;

ALTER TABLE languageingenetics.files
    ADD COLUMN IF NOT EXISTS work_version_id BIGINT;

-- Sort legacy IDs before probing the large map index; otherwise spinning disk
-- turns this into thousands of random reads.
CREATE TEMP TABLE tmp_files_to_map AS
SELECT id, article_id
FROM languageingenetics.files
WHERE article_id IS NOT NULL
  AND work_version_id IS NULL
ORDER BY article_id;

ANALYZE tmp_files_to_map;

SET enable_hashjoin = off;
SET enable_mergejoin = off;

UPDATE languageingenetics.files f
SET
    work_id = m.work_id,
    work_version_id = m.work_version_id
FROM tmp_files_to_map t
JOIN public.crossref_legacy_raw_text_map m
  ON m.raw_text_data_id = t.article_id
WHERE f.id = t.id;

RESET enable_hashjoin;
RESET enable_mergejoin;

DROP TABLE tmp_files_to_map;

CREATE INDEX IF NOT EXISTS idx_files_work_version_id
    ON languageingenetics.files (work_version_id)
    WHERE work_version_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_files_work_id
    ON languageingenetics.files (work_id)
    WHERE work_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_files_processed_work_version
    ON languageingenetics.files (processed, work_version_id)
    WHERE work_version_id IS NOT NULL;

ALTER TABLE IF EXISTS languageingenetics.batch_diagnostics
    ALTER COLUMN article_id TYPE BIGINT;

-- Helps daily dashboard counts and bulkquery's current-work selection for the
-- language-in-genetics project journals without building a corpus-wide index
-- while the annual import is active.
-- Keep this outside an explicit transaction because CONCURRENTLY requires it.
CREATE INDEX CONCURRENTLY IF NOT EXISTS crossref_work_versions_current_lig_journal_year_idx
    ON public.crossref_work_versions (journal_name, pub_year DESC, id)
    INCLUDE (work_id)
    TABLESPACE crossref_space
    WHERE is_current
      AND journal_name IN (
          'American Journal of Medical Genetics Part A',
          'American Journal of Medical Genetics Part B: Neuropsychiatric Genetics',
          'American Journal of Medical Genetics Part C: Seminars in Medical Genetics',
          'Clinical Genetics',
          'European Journal of Human Genetics',
          'Familial Cancer',
          'Genetic Epidemiology',
          'Genetics in Medicine',
          'Heredity',
          'Human Genetics',
          'Human Genetics and Genomics Advances',
          'Human Genomics',
          'Human Mutation',
          'Journal of Community Genetics',
          'Journal of Genetic Counseling',
          'Journal of Medical Genetics',
          'The American Journal of Human Genetics'
      );
