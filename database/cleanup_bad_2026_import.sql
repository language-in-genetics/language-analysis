-- Remove the aborted 2026 annual import that used raw JSON hashes as version
-- identity. The 2025 backfill ended at crossref_work_versions.id 167008748.
--
-- Run as languageingenetics against PGDATABASE=crossref.

\timing on

SET statement_timeout = 0;
SET lock_timeout = '30s';
SET enable_seqscan = off;

BEGIN;

CREATE TEMP TABLE cleanup_run2_versions AS
SELECT id, work_id
FROM public.crossref_work_versions
WHERE id BETWEEN 167008749 AND 169795076
ORDER BY id;

CREATE INDEX cleanup_run2_versions_work_id_idx
    ON cleanup_run2_versions (work_id);

CREATE INDEX cleanup_run2_versions_id_idx
    ON cleanup_run2_versions (id);

ANALYZE cleanup_run2_versions;

CREATE TEMP TABLE cleanup_run2_works AS
SELECT DISTINCT work_id
FROM cleanup_run2_versions;

CREATE UNIQUE INDEX cleanup_run2_works_work_id_idx
    ON cleanup_run2_works (work_id);

ANALYZE cleanup_run2_works;

CREATE TEMP TABLE cleanup_restore_versions AS
SELECT DISTINCT ON (v.work_id)
    v.id,
    v.work_id
FROM public.crossref_work_versions v
JOIN cleanup_run2_works w
  ON w.work_id = v.work_id
WHERE v.id <= 167008748
ORDER BY v.work_id, v.id DESC;

CREATE UNIQUE INDEX cleanup_restore_versions_work_id_idx
    ON cleanup_restore_versions (work_id);

ANALYZE cleanup_restore_versions;

-- Existing analysis rows should not point at the bad 2026 versions, but make
-- the cleanup safe if a dry run or cron path created any references.
UPDATE languageingenetics.files f
SET
    work_id = r.work_id,
    work_version_id = r.id
FROM cleanup_run2_versions bad
JOIN cleanup_restore_versions r
  ON r.work_id = bad.work_id
WHERE f.work_version_id = bad.id;

DELETE FROM languageingenetics.files f
USING cleanup_run2_versions bad
WHERE f.work_version_id = bad.id;

UPDATE public.crossref_work_versions v
SET is_current = false
FROM cleanup_run2_versions bad
WHERE v.id = bad.id
  AND v.is_current;

UPDATE public.crossref_work_versions v
SET is_current = true
FROM cleanup_restore_versions r
WHERE v.id = r.id
  AND NOT v.is_current;

DELETE FROM public.crossref_import_rejections
WHERE import_run_id = 2;

DELETE FROM public.crossref_work_versions v
USING cleanup_run2_versions bad
WHERE v.id = bad.id;

DELETE FROM public.crossref_works w
USING cleanup_run2_works bad
WHERE w.id = bad.work_id
  AND w.first_import_run_id = 2
  AND NOT EXISTS (
      SELECT 1
      FROM public.crossref_work_versions v
      WHERE v.work_id = w.id
  );

UPDATE public.crossref_works w
SET
    latest_import_run_id = 1,
    updated_at = now()
FROM cleanup_run2_works bad
WHERE w.id = bad.work_id
  AND w.latest_import_run_id = 2
  AND EXISTS (
      SELECT 1
      FROM public.crossref_work_versions v
      WHERE v.work_id = w.id
  );

UPDATE public.crossref_import_runs
SET
    status = 'failed',
    completed_at = now(),
    notes = concat_ws(
        E'\n',
        nullif(notes, ''),
        'aborted and cleaned up: raw JSON hash treated Crossref volatile metadata as new versions'
    )
WHERE id = 2;

COMMIT;

RESET enable_seqscan;

ANALYZE public.crossref_works;
ANALYZE public.crossref_work_versions;
ANALYZE languageingenetics.files;

SELECT id, run_label, status, completed_at, notes
FROM public.crossref_import_runs
WHERE id = 2;

SELECT count(*) AS remaining_run2_versions
FROM public.crossref_work_versions
WHERE id > 167008748;

SELECT count(*) AS files_pointing_to_deleted_range
FROM languageingenetics.files
WHERE work_version_id > 167008748;
