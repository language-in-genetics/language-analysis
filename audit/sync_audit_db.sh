#!/bin/bash
# Pull the lig audit SQLite database from merah to the local review_data cache.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_DB_DIR="${LOCAL_DB_DIR:-$SCRIPT_DIR/review_data}"
LOCAL_DB="${LOCAL_DB:-$LOCAL_DB_DIR/lig_audit.db}"
REMOTE_HOST="${REMOTE_HOST:-merah.cassia.ifost.org.au}"
REMOTE_DB="${REMOTE_DB:-/var/www/vhosts/lig.symmachus.org/db/lig_audit.db}"
REMOTE_UPLOAD_DIR="${REMOTE_UPLOAD_DIR:-/var/www/vhosts/lig.symmachus.org/htdocs/fulltext_uploads}"
LOCAL_UPLOAD_DIR="${LOCAL_UPLOAD_DIR:-$LOCAL_DB_DIR/fulltext_uploads}"
REMOTE_SNAPSHOT="${REMOTE_SNAPSHOT:-/tmp/lig_audit_snapshot_$(date +%Y%m%d%H%M%S)_$$.db}"
LOG_FILE="${LOG_FILE:-$SCRIPT_DIR/audit_sync.log}"

mkdir -p "$LOCAL_DB_DIR"
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log "=== Starting lig audit database sync ==="

if ! ssh -o BatchMode=yes "$REMOTE_HOST" "test -f '$REMOTE_DB'"; then
    log "WARNING: Remote audit database does not exist yet: $REMOTE_DB"
    exit 0
fi

REMOTE_SIZE=$(ssh -o BatchMode=yes "$REMOTE_HOST" "ls -lh '$REMOTE_DB' | awk '{print \$5}'")
log "Remote audit database size: $REMOTE_SIZE"

ssh -o BatchMode=yes "$REMOTE_HOST" "
    set -e
    rm -f '$REMOTE_SNAPSHOT'
    if sqlite3 '$REMOTE_DB' \".backup '$REMOTE_SNAPSHOT'\" 2>/dev/null; then
        chmod 0644 '$REMOTE_SNAPSHOT'
    elif command -v doas >/dev/null 2>&1; then
        doas rm -f '$REMOTE_SNAPSHOT'
        doas -u languageingenetics sqlite3 '$REMOTE_DB' \".backup '$REMOTE_SNAPSHOT'\"
        doas chmod 0644 '$REMOTE_SNAPSHOT'
    else
        exit 1
    fi
"
scp "$REMOTE_HOST:$REMOTE_SNAPSHOT" "$LOCAL_DB"
ssh -o BatchMode=yes "$REMOTE_HOST" "rm -f '$REMOTE_SNAPSHOT' 2>/dev/null || doas rm -f '$REMOTE_SNAPSHOT'"

if ssh -o BatchMode=yes "$REMOTE_HOST" "test -d '$REMOTE_UPLOAD_DIR'"; then
    mkdir -p "$LOCAL_UPLOAD_DIR"
    rsync -az --delete "$REMOTE_HOST:$REMOTE_UPLOAD_DIR/" "$LOCAL_UPLOAD_DIR/"
    log "Synced full-text uploads to $LOCAL_UPLOAD_DIR"
fi

LOCAL_SIZE=$(ls -lh "$LOCAL_DB" | awk '{print $5}')
AUDIT_COUNT=$(sqlite3 "$LOCAL_DB" "SELECT COUNT(*) FROM audit_articles WHERE target_confirmed IS NOT NULL;")
if sqlite3 "$LOCAL_DB" "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'fulltext_articles' LIMIT 1;" | grep -q 1; then
    FULLTEXT_UPLOADED_COUNT=$(sqlite3 "$LOCAL_DB" "SELECT COUNT(*) FROM fulltext_articles WHERE fulltext_status = 'available';")
    if sqlite3 "$LOCAL_DB" "PRAGMA table_info(fulltext_articles);" | awk -F'|' '{print $2}' | grep -qx ai_analysis_status; then
        FULLTEXT_AI_COUNT=$(sqlite3 "$LOCAL_DB" "SELECT COUNT(*) FROM fulltext_articles WHERE ai_analysis_status = 'processed';")
    else
        FULLTEXT_AI_COUNT=0
    fi
else
    FULLTEXT_UPLOADED_COUNT=0
    FULLTEXT_AI_COUNT=0
fi
log "Synced database to $LOCAL_DB (size: $LOCAL_SIZE, label reviewed rows: $AUDIT_COUNT, full-text uploaded rows: $FULLTEXT_UPLOADED_COUNT, full-text AI processed rows: $FULLTEXT_AI_COUNT)"
log "=== Lig audit database sync complete ==="
