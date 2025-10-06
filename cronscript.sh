#!/bin/bash
set -euo pipefail

# Configuration
WORKDIR="/home/languageingenetics/Word-Frequency-Analysis-"
DASHBOARD_DIR="$WORKDIR/dashboard"
REMOTE_HOST="merah.cassia.ifost.org.au"
REMOTE_PATH="/var/www/vhosts/lig.symmachus.org/htdocs/"
BATCH_SIZE=1000
LOG_FILE="$WORKDIR/cronscript.log"

# PostgreSQL connection uses environment variables
# PGDATABASE should be set to "crossref"
export PGDATABASE="${PGDATABASE:-crossref}"

# Change to working directory
cd "$WORKDIR"

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

# Step 2: Submit new batch for processing with query explanations
log "Submitting new batch of $BATCH_SIZE articles..."
BULKQUERY_EXPLAIN_LOG="$WORKDIR/bulkquery_explains.log"
# Clear the explain log before running to prevent unbounded growth
rm -f "$BULKQUERY_EXPLAIN_LOG"
if uv run bulkquery.py --limit "$BATCH_SIZE" --explain-queries --explain-log "$BULKQUERY_EXPLAIN_LOG" 2>&1 | tee -a "$LOG_FILE"; then
    log "New batch submitted successfully"
else
    log "Warning: No new articles to process or batch submission failed"
fi

# Step 3: Generate static dashboard with query explanations
log "Generating dashboard..."
EXPLAIN_LOG="$WORKDIR/query_explains.log"
# Clear the explain log before running to prevent unbounded growth
rm -f "$EXPLAIN_LOG"
if uv run generate_dashboard.py --output-dir "$DASHBOARD_DIR" --explain-queries --explain-log "$EXPLAIN_LOG" 2>&1 | tee -a "$LOG_FILE"; then
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

# Step 5: Analyze query performance and suggest optimizations
log "Analyzing query performance..."
OPTIMIZATION_REPORT="$WORKDIR/optimization_suggestions.md"

# Analyze dashboard query performance
if [ -f "$EXPLAIN_LOG" ]; then
    log "Running Claude Code to analyze dashboard EXPLAIN output..."
    claude code analyze "$EXPLAIN_LOG" --prompt "Review these PostgreSQL EXPLAIN (ANALYZE, BUFFERS, VERBOSE) outputs from generate_dashboard.py and provide specific recommendations for database optimization. For each query, identify:\n1. Missing indexes that would improve performance\n2. Inefficient query patterns (sequential scans on large tables, expensive operations)\n3. Specific CREATE INDEX statements to run\n4. Code refactoring suggestions if applicable\n\nFormat your response as a markdown report with actionable recommendations." > "$OPTIMIZATION_REPORT" 2>&1 || log "Warning: Claude Code analysis failed or not available"

    if [ -f "$OPTIMIZATION_REPORT" ] && [ -s "$OPTIMIZATION_REPORT" ]; then
        log "Dashboard optimization suggestions written to $OPTIMIZATION_REPORT"
    fi
else
    log "No dashboard EXPLAIN log found, skipping dashboard performance analysis"
fi

# Analyze bulkquery performance
BULKQUERY_OPTIMIZATION_REPORT="$WORKDIR/bulkquery_optimization_suggestions.md"
if [ -f "$BULKQUERY_EXPLAIN_LOG" ]; then
    log "Running Claude Code to analyze bulkquery EXPLAIN output..."
    claude code analyze "$BULKQUERY_EXPLAIN_LOG" --prompt "Review these PostgreSQL EXPLAIN (ANALYZE, BUFFERS, VERBOSE) outputs from bulkquery.py and provide specific recommendations for database optimization. This script processes articles from the raw_text_data table which is hundreds of GB in size. For each query, identify:\n1. Missing indexes that would improve performance (especially for the JSONB queries on filesrc column)\n2. Inefficient query patterns (sequential scans on large tables, expensive operations)\n3. Specific CREATE INDEX statements to run\n4. Code refactoring suggestions if applicable\n5. Whether existing indexes are being used effectively\n\nFormat your response as a markdown report with actionable recommendations. Priority should be given to optimizations that will save hours of processing time." > "$BULKQUERY_OPTIMIZATION_REPORT" 2>&1 || log "Warning: Claude Code analysis failed or not available"

    if [ -f "$BULKQUERY_OPTIMIZATION_REPORT" ] && [ -s "$BULKQUERY_OPTIMIZATION_REPORT" ]; then
        log "Bulkquery optimization suggestions written to $BULKQUERY_OPTIMIZATION_REPORT"
    fi
else
    log "No bulkquery EXPLAIN log found, skipping bulkquery performance analysis"
fi

log "Automated processing run completed successfully"
log "---"
