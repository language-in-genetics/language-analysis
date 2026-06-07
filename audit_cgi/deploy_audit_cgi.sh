#!/bin/bash
# Copy LIG audit CGI source to merah, build it on-host, and install binaries.

set -euo pipefail

REMOTE_HOST="${1:-merah.cassia.ifost.org.au}"
REMOTE_CGI_DIR="${2:-/var/www/vhosts/lig.symmachus.org/cgi-bin}"
REMOTE_BUILD_DIR="${3:-/home/gregb/lig-audit-build}"
REMOTE_DB="${REMOTE_DB:-/var/www/vhosts/lig.symmachus.org/db/lig_audit.db}"
REMOTE_DB_DIR="$(dirname "$REMOTE_DB")"
REMOTE_MIGRATION_DIR="${REMOTE_MIGRATION_DIR:-/tmp/lig-audit-migrate}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SSH_OPTS=(-o ServerAliveInterval=30 -o ServerAliveCountMax=12)

log() {
    printf '[lig-deploy] %s\n' "$*"
}

log "preparing remote build directory: $REMOTE_HOST:$REMOTE_BUILD_DIR"
ssh "${SSH_OPTS[@]}" "$REMOTE_HOST" "mkdir -p '$REMOTE_BUILD_DIR'"

log "copying CGI sources"
scp -o ServerAliveInterval=30 -o ServerAliveCountMax=12 "$SCRIPT_DIR"/*.go "$SCRIPT_DIR"/go.mod "$SCRIPT_DIR"/go.sum "$SCRIPT_DIR"/Makefile "$SCRIPT_DIR"/init_schema.sql "$SCRIPT_DIR"/migrate_sqlite_schema.sh "$REMOTE_HOST":"$REMOTE_BUILD_DIR"/

log "building and installing on remote host"
ssh "${SSH_OPTS[@]}" "$REMOTE_HOST" "
    set -e
    cd '$REMOTE_BUILD_DIR'
    step() { printf '[lig-deploy] %s\n' \"\$1\"; }
    build() { output=\"\$1\"; shift; step \"build \$output\"; CGO_ENABLED=1 /usr/local/bin/go build -o \"\$output\" \"\$@\"; }
    install_cgi() { name=\"\$1\"; step \"install \$name\"; doas install -o languageingenetics -g daemon -m 755 \"\$name\" '$REMOTE_CGI_DIR'/\"\$name\"; }

    build audit.cgi audit.go common.go
    build audit-save.cgi audit_save.go common.go
    build audit-status.cgi audit_status.go common.go
    build fulltext-audit.cgi fulltext_audit.go common.go fulltext_common.go
    build fulltext-verify.cgi fulltext_audit.go common.go fulltext_common.go
    build fulltext-save.cgi fulltext_save.go common.go fulltext_common.go
    build fulltext-status.cgi fulltext_status.go common.go fulltext_common.go
    build fulltext-upload.cgi fulltext_upload.go common.go fulltext_common.go
    build audit-human-subject.cgi human_subject_audit.go common.go human_subject_common.go
    build audit-human-subject-save.cgi human_subject_save.go common.go human_subject_common.go
    build audit-human-subject-status.cgi human_subject_status.go common.go human_subject_common.go
    step 'prepare directories'
    doas mkdir -p '$REMOTE_CGI_DIR' '$REMOTE_DB_DIR' /var/www/vhosts/lig.symmachus.org/etc
    doas chown languageingenetics:www '$REMOTE_DB_DIR'
    doas chmod 775 '$REMOTE_DB_DIR'
    doas mkdir -p '$REMOTE_MIGRATION_DIR'
    step 'install migration files'
    doas install -o languageingenetics -g daemon -m 644 init_schema.sql '$REMOTE_MIGRATION_DIR/init_schema.sql'
    doas install -o languageingenetics -g daemon -m 755 migrate_sqlite_schema.sh '$REMOTE_MIGRATION_DIR/migrate_sqlite_schema.sh'
    step 'migrate SQLite schema'
    doas -u languageingenetics sh '$REMOTE_MIGRATION_DIR/migrate_sqlite_schema.sh' '$REMOTE_DB'
    install_cgi audit.cgi
    install_cgi audit-save.cgi
    install_cgi audit-status.cgi
    install_cgi fulltext-audit.cgi
    install_cgi fulltext-verify.cgi
    install_cgi fulltext-save.cgi
    install_cgi fulltext-status.cgi
    install_cgi fulltext-upload.cgi
    install_cgi audit-human-subject.cgi
    install_cgi audit-human-subject-save.cgi
    install_cgi audit-human-subject-status.cgi
    step 'done'
"
