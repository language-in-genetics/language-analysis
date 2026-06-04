#!/bin/bash

set -euo pipefail

# Configuration
WORKDIR="/home/languageingenetics/Word-Frequency-Analysis-"
DASHBOARD_DIR="$WORKDIR/dashboard"
REMOTE_HOST="merah.cassia.ifost.org.au"
REMOTE_PATH="/var/www/vhosts/lig.symmachus.org/htdocs/"
BATCH_SIZE=4000
HUMAN_SUBJECT_BATCH_SIZE="${HUMAN_SUBJECT_BATCH_SIZE:-$BATCH_SIZE}"
LOG_FILE="$WORKDIR/cronscript.log"
OPENAI_API_KEY_FILE="${OPENAI_API_KEY_FILE:-/home/languageingenetics/.openai.lig.key}"
# Leave empty to process all publication years. Set TARGET_PUB_YEAR in the
# environment only for a deliberate year-specific backfill.
TARGET_PUB_YEAR="${TARGET_PUB_YEAR:-}"
if [[ -z "${CROSSREF_RETRACTION_SOURCE_SQLITE:-}" ]]; then
    for candidate in \
        "/dbtemp/March 2026 Public Data File from Crossref/_focused_journals_doi/focused-journals.sqlite" \
        "/dbtemp/March 2026 Public Data File from Crossref/_focused_journals_doi_rebuilt_20260506/focused-journals.sqlite"; do
        if [[ -s "$candidate" ]]; then
            export CROSSREF_RETRACTION_SOURCE_SQLITE="$candidate"
            break
        fi
    done
fi
if [[ -z "${CROSSREF_RETRACTION_SOURCE_JSONL_GZ:-}" ]]; then
    for candidate in \
        "/dbtemp/March 2026 Public Data File from Crossref/_focused_journals_doi_rebuilt_20260506/focused-journals-doi.jsonl.gz" \
        "/dbtemp/March 2026 Public Data File from Crossref/_focused_journals_doi/focused-journals-doi.jsonl.gz"; do
        if [[ -s "$candidate" ]]; then
            export CROSSREF_RETRACTION_SOURCE_JSONL_GZ="$candidate"
            break
        fi
    done
fi

# PostgreSQL connection uses environment variables
# PGDATABASE should be set to "crossref"
export PGDATABASE="${PGDATABASE:-crossref}"
export PGHOST="${PGHOST:-/var/run/postgresql}"

# Change to working directory
cd "$WORKDIR"

git pull -q || true
#echo "Disabled"
#exit 0

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log "Starting automated processing run"

# Step 0: Sync local audit cache from merah and import into PostgreSQL
log "Syncing lig audit database from merah..."
AUDIT_SYNC_OK=0
if ./audit/sync_audit_db.sh 2>&1 | tee -a "$LOG_FILE"; then
    log "Lig audit database sync completed"
    AUDIT_SYNC_OK=1
else
    log "Warning: Lig audit database sync failed"
fi

log "Importing lig audit reviews into PostgreSQL..."
if (
    cd extractor
    uv run import_audit_reviews.py --sqlite-db ../audit/review_data/lig_audit.db
) 2>&1 | tee -a "$LOG_FILE"; then
    log "Lig audit import completed"
else
    log "Warning: Lig audit import failed"
fi

log "Importing lig full-text upload and AI processing state into PostgreSQL..."
if (
    cd extractor
    uv run import_fulltext_audit_reviews.py --sqlite-db ../audit/review_data/lig_audit.db
) 2>&1 | tee -a "$LOG_FILE"; then
    log "Lig full-text upload state import completed"
else
    log "Warning: Lig full-text upload state import failed"
fi

log "Processing queued full-text uploads..."
if (
    cd extractor
    uv run process_fulltext_analysis.py \
        --sqlite-db ../audit/review_data/lig_audit.db \
        --openai-api-key "$OPENAI_API_KEY_FILE" \
        --limit "${FULLTEXT_ANALYSIS_LIMIT:-10}"
) 2>&1 | tee -a "$LOG_FILE"; then
    log "Queued full-text upload processing completed"
else
    log "Warning: queued full-text upload processing had errors"
fi

if [[ "$AUDIT_SYNC_OK" -eq 1 ]]; then
    log "Pushing processed lig audit database back to merah..."
    if ./audit/push_audit_db.sh 2>&1 | tee -a "$LOG_FILE"; then
        log "Processed lig audit database push completed"
    else
        log "Warning: processed lig audit database push failed"
    fi
else
    log "Skipping lig audit database push because the initial sync failed"
fi

log "Generating full-text AI report..."
if (
    cd extractor
    uv run generate_fulltext_report.py --output "$DASHBOARD_DIR/fulltext.html"
) 2>&1 | tee -a "$LOG_FILE"; then
    log "Full-text AI report generated successfully"
    if rsync -avz "$DASHBOARD_DIR/fulltext.html" "$REMOTE_HOST:$REMOTE_PATH/fulltext.html" 2>&1 | tee -a "$LOG_FILE"; then
        log "Full-text AI report synced successfully to $REMOTE_HOST:$REMOTE_PATH/fulltext.html"
    else
        log "Warning: full-text AI report sync failed"
    fi
else
    log "Warning: full-text AI report generation failed"
fi

# Step 1: Check for completed batches and fetch results
log "Checking for completed batches..."
cd extractor
if uv run batchfetch.py --openai-api-key "$OPENAI_API_KEY_FILE" --report-costs 2>&1 | tee -a "$LOG_FILE"; then
    log "Batch fetch completed successfully"
else
    log "Warning: Batch fetch had errors (may be normal if no batches ready)"
fi

log "Checking for completed Homo sapiens filter batches..."
if uv run human_subject_batchfetch.py --openai-api-key "$OPENAI_API_KEY_FILE" --report-costs 2>&1 | tee -a "$LOG_FILE"; then
    log "Homo sapiens filter batch fetch completed successfully"
else
    log "Warning: Homo sapiens filter batch fetch had errors (may be normal if no batches ready)"
fi

# Step 2: Submit new batch for processing
bulkquery_year_args=()
if [[ -n "$TARGET_PUB_YEAR" ]]; then
    bulkquery_year_args=(--pub-year "$TARGET_PUB_YEAR")
fi

log "Submitting new batch of $BATCH_SIZE articles${TARGET_PUB_YEAR:+ for publication year $TARGET_PUB_YEAR}..."
if uv run bulkquery.py --openai-api-key "$OPENAI_API_KEY_FILE" --limit "$BATCH_SIZE" "${bulkquery_year_args[@]}" 2>&1 | tee -a "$LOG_FILE"; then
    log "New batch submitted successfully"
else
    log "Warning: No new articles to process or batch submission failed"
fi

if [[ "$HUMAN_SUBJECT_BATCH_SIZE" -gt 0 ]]; then
    log "Submitting new Homo sapiens filter batch of $HUMAN_SUBJECT_BATCH_SIZE articles${TARGET_PUB_YEAR:+ for publication year $TARGET_PUB_YEAR}..."
    if uv run human_subject_bulkquery.py --openai-api-key "$OPENAI_API_KEY_FILE" --limit "$HUMAN_SUBJECT_BATCH_SIZE" "${bulkquery_year_args[@]}" 2>&1 | tee -a "$LOG_FILE"; then
        log "New Homo sapiens filter batch submitted successfully"
    else
        log "Warning: No new Homo sapiens filter articles to process or batch submission failed"
    fi
else
    log "Skipping Homo sapiens filter batch submission because HUMAN_SUBJECT_BATCH_SIZE=$HUMAN_SUBJECT_BATCH_SIZE"
fi

# Step 3: Generate static dashboard
log "Generating dashboard..."
if uv run generate_dashboard.py --output-dir "$DASHBOARD_DIR" 2>&1 | tee -a "$LOG_FILE"; then
    log "Dashboard generated successfully"
else
    log "Error: Dashboard generation failed"
    exit 1
fi

# Step 4: Sync dashboard to remote server
log "Syncing dashboard to remote server..."
if rsync -avz --delete "$DASHBOARD_DIR/" "$REMOTE_HOST:$REMOTE_PATH" 2>&1 | tee -a "$LOG_FILE"; then
    log "Dashboard synced successfully to $REMOTE_HOST:$REMOTE_PATH"
else
    log "Error: Dashboard sync failed"
    exit 1
fi

log "Automated processing run completed successfully"
log "---"
