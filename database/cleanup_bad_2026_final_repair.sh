#!/usr/bin/env bash
set -euo pipefail

export PGDATABASE=${PGDATABASE:-crossref}
export PGHOST=${PGHOST:-/var/run/postgresql}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

psql_run() {
  psql -X -v ON_ERROR_STOP=1 "$@"
}

log "Starting bad 2026 import final repair"
log "Removing any remaining run-2-only works without versions"
psql_run -c "
DELETE FROM public.crossref_works w
WHERE w.first_import_run_id = 2
  AND w.latest_import_run_id = 2
  AND NOT EXISTS (
      SELECT 1
      FROM public.crossref_work_versions v
      WHERE v.work_id = w.id
  );
"

chunk_size=50000
chunk=0
while :; do
  chunk=$((chunk + 1))
  updated=$(psql_run -qAt -c "
WITH candidates AS (
    SELECT id
    FROM public.crossref_works
    WHERE first_import_run_id = 1
      AND latest_import_run_id = 2
    ORDER BY id
    LIMIT ${chunk_size}
), updated AS (
    UPDATE public.crossref_works w
    SET latest_import_run_id = 1,
        updated_at = now()
    FROM candidates c
    WHERE w.id = c.id
    RETURNING 1
)
SELECT count(*) FROM updated;
")
  log "Restored latest_import_run_id chunk ${chunk}: ${updated} rows"
  if [ "$updated" = "0" ]; then
    break
  fi
  sleep 1
done

log "Marking import run 2 as failed/cleaned"
psql_run -c "
UPDATE public.crossref_import_runs
SET status = 'failed',
    completed_at = COALESCE(completed_at, now()),
    notes = concat_ws(
        E'\n',
        nullif(notes, ''),
        'aborted and cleaned up: raw JSON hash treated Crossref volatile metadata as new versions'
    )
WHERE id = 2;
"

log "Analyzing repaired tables"
psql_run -c "ANALYZE public.crossref_works;"
psql_run -c "ANALYZE public.crossref_work_versions;"
psql_run -c "ANALYZE languageingenetics.files;"

log "Verification snapshot"
psql_run -x -c "
SELECT id, run_label, status, completed_at, notes
FROM public.crossref_import_runs
WHERE id = 2;
"
psql_run -x -c "
SELECT EXISTS (
    SELECT 1 FROM public.crossref_work_versions
    WHERE id BETWEEN 167008749 AND 169795076
    LIMIT 1
) AS bad_versions_remain;
"
psql_run -x -c "
SELECT EXISTS (
    SELECT 1 FROM languageingenetics.files
    WHERE work_version_id BETWEEN 167008749 AND 169795076
    LIMIT 1
) AS files_point_to_bad_range;
"
log "Bad 2026 import final repair complete"
