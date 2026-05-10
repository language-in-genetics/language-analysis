#!/bin/bash
# Copy LIG audit CGI source to merah, build it on-host, and install binaries.

set -euo pipefail

REMOTE_HOST="${1:-merah.cassia.ifost.org.au}"
REMOTE_CGI_DIR="${2:-/var/www/vhosts/lig.symmachus.org/cgi-bin}"
REMOTE_BUILD_DIR="${3:-/home/gregb/lig-audit-build}"
REMOTE_DB="${REMOTE_DB:-/var/www/vhosts/lig.symmachus.org/db/lig_audit.db}"
REMOTE_DB_DIR="$(dirname "$REMOTE_DB")"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

ssh "$REMOTE_HOST" "mkdir -p '$REMOTE_BUILD_DIR'"
scp "$SCRIPT_DIR"/*.go "$SCRIPT_DIR"/go.mod "$SCRIPT_DIR"/go.sum "$SCRIPT_DIR"/Makefile "$SCRIPT_DIR"/init_schema.sql "$REMOTE_HOST":"$REMOTE_BUILD_DIR"/
ssh "$REMOTE_HOST" "
    set -e
    cd '$REMOTE_BUILD_DIR'
    CGO_ENABLED=1 /usr/local/bin/go build -o audit.cgi audit.go common.go
    CGO_ENABLED=1 /usr/local/bin/go build -o audit-save.cgi audit_save.go common.go
    CGO_ENABLED=1 /usr/local/bin/go build -o audit-status.cgi audit_status.go common.go
    CGO_ENABLED=1 /usr/local/bin/go build -o fulltext-audit.cgi fulltext_audit.go common.go fulltext_common.go
    CGO_ENABLED=1 /usr/local/bin/go build -o fulltext-save.cgi fulltext_save.go common.go fulltext_common.go
    CGO_ENABLED=1 /usr/local/bin/go build -o fulltext-status.cgi fulltext_status.go common.go fulltext_common.go
    doas mkdir -p '$REMOTE_CGI_DIR' '$REMOTE_DB_DIR' /var/www/vhosts/lig.symmachus.org/etc
    doas install -o languageingenetics -g daemon -m 755 audit.cgi '$REMOTE_CGI_DIR/audit.cgi'
    doas install -o languageingenetics -g daemon -m 755 audit-save.cgi '$REMOTE_CGI_DIR/audit-save.cgi'
    doas install -o languageingenetics -g daemon -m 755 audit-status.cgi '$REMOTE_CGI_DIR/audit-status.cgi'
    doas install -o languageingenetics -g daemon -m 755 fulltext-audit.cgi '$REMOTE_CGI_DIR/fulltext-audit.cgi'
    doas install -o languageingenetics -g daemon -m 755 fulltext-save.cgi '$REMOTE_CGI_DIR/fulltext-save.cgi'
    doas install -o languageingenetics -g daemon -m 755 fulltext-status.cgi '$REMOTE_CGI_DIR/fulltext-status.cgi'
    doas -u languageingenetics sqlite3 '$REMOTE_DB' < init_schema.sql
"
