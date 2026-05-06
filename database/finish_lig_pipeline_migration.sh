#!/usr/bin/env bash
set -euo pipefail

cd /home/languageingenetics/Word-Frequency-Analysis-

export PGDATABASE="${PGDATABASE:-crossref}"
export PGHOST="${PGHOST:-/var/run/postgresql}"
export OPENAI_API_KEY_FILE="${OPENAI_API_KEY_FILE:-/home/languageingenetics/.openai.lig.key}"

LOG=/home/languageingenetics/Word-Frequency-Analysis-/logs/finish_lig_pipeline_migration.log

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"
}

enable_daily_cron() {
    local crontab_file
    crontab_file=$(mktemp)
    crontab -l > "$crontab_file" || true

    if grep -q '^#15 23 \* \* \* /home/languageingenetics/Word-Frequency-Analysis-/cronscript.sh' "$crontab_file"; then
        sed -i 's|^#15 23 \* \* \* /home/languageingenetics/Word-Frequency-Analysis-/cronscript.sh|15 23 * * * /home/languageingenetics/Word-Frequency-Analysis-/cronscript.sh|' "$crontab_file"
    elif ! grep -q '^15 23 \* \* \* /home/languageingenetics/Word-Frequency-Analysis-/cronscript.sh' "$crontab_file"; then
        echo '15 23 * * * /home/languageingenetics/Word-Frequency-Analysis-/cronscript.sh' >> "$crontab_file"
    fi

    crontab "$crontab_file"
    rm -f "$crontab_file"
}

log "Starting LIG pipeline finish job"

invalid_index=$(
    psql -Atc "
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE n.nspname = 'public'
          AND c.relname = 'crossref_work_versions_current_lig_journal_year_idx'
          AND NOT i.indisvalid
        LIMIT 1
    "
)

if [[ "$invalid_index" == "1" ]]; then
    log "Dropping invalid prior current-work index"
    psql -v ON_ERROR_STOP=1 \
        -c "DROP INDEX CONCURRENTLY public.crossref_work_versions_current_lig_journal_year_idx"
fi

log "Creating current-work journal/year index if needed"
psql -v ON_ERROR_STOP=1 <<'SQL'
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
SQL

log "Running dry-run 2025 bulk query smoke test"
(
    cd extractor
    uv run bulkquery.py \
        --dry-run \
        --limit 1 \
        --pub-year 2025 \
        --openai-api-key "$OPENAI_API_KEY_FILE"
) 2>&1 | tee -a "$LOG"

log "Running dashboard generation smoke test"
(
    cd extractor
    uv run generate_dashboard.py --output-dir /tmp/lig-dashboard-test
) 2>&1 | tee -a "$LOG"

log "Running one full project cron pass"
./cronscript.sh 2>&1 | tee -a "$LOG"

log "Re-enabling daily cronscript line"
enable_daily_cron

log "LIG pipeline finish job completed"
