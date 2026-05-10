#!/bin/bash
# Push the local LIG audit SQLite database to merah after seeding a new batch.
#
# This is intentionally separate from CGI deployment. It overwrites the live
# review database, so run sync_audit_db.sh first if there may be newer reviews
# on merah.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_DB="${LOCAL_DB:-$SCRIPT_DIR/review_data/lig_audit.db}"
REMOTE_HOST="${REMOTE_HOST:-merah.cassia.ifost.org.au}"
REMOTE_DB="${REMOTE_DB:-/var/www/vhosts/lig.symmachus.org/db/lig_audit.db}"
REMOTE_DB_DIR="$(dirname "$REMOTE_DB")"
REMOTE_BACKUP_DIR="${REMOTE_BACKUP_DIR:-$REMOTE_DB_DIR/backups}"
REMOTE_TMP="${REMOTE_TMP:-/tmp/lig_audit_push_$(date +%Y%m%d%H%M%S)_$$.db}"
LOG_FILE="${LOG_FILE:-$SCRIPT_DIR/audit_sync.log}"

mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

if [[ ! -s "$LOCAL_DB" ]]; then
    log "ERROR: local audit database does not exist or is empty: $LOCAL_DB"
    exit 1
fi

log "=== Starting lig audit database push ==="
LOCAL_SIZE=$(ls -lh "$LOCAL_DB" | awk '{print $5}')
log "Local audit database: $LOCAL_DB (size: $LOCAL_SIZE)"

scp "$LOCAL_DB" "$REMOTE_HOST:$REMOTE_TMP"

ssh -o BatchMode=yes "$REMOTE_HOST" "
    set -e
    doas mkdir -p '$REMOTE_DB_DIR' '$REMOTE_BACKUP_DIR'
    doas chown languageingenetics:www '$REMOTE_DB_DIR' '$REMOTE_BACKUP_DIR'
    if test -f '$REMOTE_DB'; then
        doas -u languageingenetics sqlite3 '$REMOTE_DB' 'PRAGMA wal_checkpoint(TRUNCATE);' >/dev/null
        doas -u languageingenetics sqlite3 '$REMOTE_DB' \".backup '$REMOTE_BACKUP_DIR/lig_audit_$(date +%Y%m%d%H%M%S).db'\"
    fi
    doas rm -f '$REMOTE_DB' '$REMOTE_DB-wal' '$REMOTE_DB-shm'
    doas install -o languageingenetics -g www -m 664 '$REMOTE_TMP' '$REMOTE_DB'
    rm -f '$REMOTE_TMP'
    doas -u languageingenetics sqlite3 '$REMOTE_DB' < /dev/null
"

log "Pushed audit database to $REMOTE_HOST:$REMOTE_DB"
log "=== Lig audit database push complete ==="
