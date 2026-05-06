#!/usr/bin/env bash
set -euo pipefail

export PGDATABASE="${PGDATABASE:-crossref}"
export PGHOST="${PGHOST:-/var/run/postgresql}"

start_id=167008749
end_id=169795076
chunk_size="${CHUNK_SIZE:-100000}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

run_sql() {
    /usr/bin/psql -v ON_ERROR_STOP=1 "$@"
}

log "Starting bad 2026 import cleanup over IDs ${start_id}-${end_id}"

for ((lo = start_id; lo <= end_id; lo += chunk_size)); do
    hi=$((lo + chunk_size - 1))
    if ((hi > end_id)); then
        hi=$end_id
    fi
    log "Restoring/deleting bad versions ${lo}-${hi}"
    run_sql <<SQL
SET enable_seqscan = off;
BEGIN;

CREATE TEMP TABLE cleanup_bad AS
SELECT id, work_id
FROM public.crossref_work_versions
WHERE id BETWEEN ${lo} AND ${hi}
ORDER BY id;

CREATE INDEX cleanup_bad_work_id_idx ON cleanup_bad (work_id);
CREATE INDEX cleanup_bad_id_idx ON cleanup_bad (id);
ANALYZE cleanup_bad;

CREATE TEMP TABLE cleanup_restore AS
SELECT DISTINCT ON (v.work_id)
    v.id,
    v.work_id
FROM public.crossref_work_versions v
JOIN (
    SELECT DISTINCT work_id
    FROM cleanup_bad
) bad
  ON bad.work_id = v.work_id
WHERE v.id <= 167008748
ORDER BY v.work_id, v.id DESC;

CREATE UNIQUE INDEX cleanup_restore_work_id_idx ON cleanup_restore (work_id);
ANALYZE cleanup_restore;

UPDATE languageingenetics.files f
SET
    work_id = r.work_id,
    work_version_id = r.id
FROM cleanup_bad bad
JOIN cleanup_restore r
  ON r.work_id = bad.work_id
WHERE f.work_version_id = bad.id;

DELETE FROM languageingenetics.files f
USING cleanup_bad bad
WHERE f.work_version_id = bad.id;

DELETE FROM public.crossref_work_versions v
USING cleanup_bad bad
WHERE v.id = bad.id;

UPDATE public.crossref_work_versions v
SET is_current = true
FROM cleanup_restore r
WHERE v.id = r.id
  AND NOT v.is_current
  AND NOT EXISTS (
      SELECT 1
      FROM public.crossref_work_versions current_v
      WHERE current_v.work_id = r.work_id
        AND current_v.is_current
  );

COMMIT;
SQL
done

log "Removing run-2-only works and repairing import-run metadata"
run_sql <<'SQL'
SET enable_seqscan = off;
BEGIN;

DELETE FROM public.crossref_import_rejections
WHERE import_run_id = 2;

DELETE FROM public.crossref_works w
WHERE w.first_import_run_id = 2
  AND w.latest_import_run_id = 2
  AND NOT EXISTS (
      SELECT 1
      FROM public.crossref_work_versions v
      WHERE v.work_id = w.id
  );

UPDATE public.crossref_works w
SET
    latest_import_run_id = 1,
    updated_at = now()
WHERE w.latest_import_run_id = 2
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

ANALYZE public.crossref_works;
ANALYZE public.crossref_work_versions;
ANALYZE languageingenetics.files;

SELECT id, run_label, status, completed_at, notes
FROM public.crossref_import_runs
WHERE id = 2;

SELECT count(*) AS remaining_bad_versions
FROM public.crossref_work_versions
WHERE id BETWEEN 167008749 AND 169795076;

SELECT count(*) AS files_pointing_to_bad_range
FROM languageingenetics.files
WHERE work_version_id BETWEEN 167008749 AND 169795076;
SQL

log "Bad 2026 import cleanup completed"
