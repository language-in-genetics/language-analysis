#!/bin/bash
# Pull the lig audit SQLite database from merah to the local review_data cache.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_DB_DIR="${LOCAL_DB_DIR:-$SCRIPT_DIR/review_data}"
LOCAL_DB="${LOCAL_DB:-$LOCAL_DB_DIR/lig_audit.db}"
REMOTE_HOST="${REMOTE_HOST:-merah.cassia.ifost.org.au}"
REMOTE_DB="${REMOTE_DB:-/var/www/vhosts/lig.symmachus.org/db/lig_audit.db}"
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

scp "$REMOTE_HOST:$REMOTE_DB" "$LOCAL_DB"

LOCAL_SIZE=$(ls -lh "$LOCAL_DB" | awk '{print $5}')
AUDIT_COUNT=$(sqlite3 "$LOCAL_DB" "SELECT COUNT(*) FROM audit_articles WHERE target_confirmed IS NOT NULL;")
log "Synced database to $LOCAL_DB (size: $LOCAL_SIZE, reviewed rows: $AUDIT_COUNT)"
log "=== Lig audit database sync complete ==="
