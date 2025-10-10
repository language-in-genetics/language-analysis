#!/bin/bash

set -euo pipefail

# Configuration
WORKDIR="/home/languageingenetics/Word-Frequency-Analysis-"
DASHBOARD_DIR="$WORKDIR/dashboard"
REMOTE_HOST="merah.cassia.ifost.org.au"
REMOTE_PATH="/var/www/vhosts/lig.symmachus.org/htdocs/"
BATCH_SIZE=2000
LOG_FILE="$WORKDIR/cronscript.log"

# PostgreSQL connection uses environment variables
# PGDATABASE should be set to "crossref"
export PGDATABASE="${PGDATABASE:-crossref}"

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

# Step 1: Check for completed batches and fetch results
log "Checking for completed batches..."
cd extractor
if uv run batchfetch.py --report-costs 2>&1 | tee -a "$LOG_FILE"; then
    log "Batch fetch completed successfully"
else
    log "Warning: Batch fetch had errors (may be normal if no batches ready)"
fi

# Step 2: Submit new batch for processing
log "Submitting new batch of $BATCH_SIZE articles..."
if uv run bulkquery.py --limit "$BATCH_SIZE" 2>&1 | tee -a "$LOG_FILE"; then
    log "New batch submitted successfully"
else
    log "Warning: No new articles to process or batch submission failed"
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
