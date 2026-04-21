#!/bin/bash
# Copy lig audit CGI source to merah and build it in place.

set -euo pipefail

REMOTE_HOST="${1:-merah.cassia.ifost.org.au}"
REMOTE_CGI_DIR="${2:-/var/www/vhosts/lig.symmachus.org/cgi-bin}"
REMOTE_BUILD_DIR="${3:-/home/gregb/lig-audit-build}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

ssh "$REMOTE_HOST" "mkdir -p '$REMOTE_BUILD_DIR' '$REMOTE_CGI_DIR' /var/www/vhosts/lig.symmachus.org/db /var/www/vhosts/lig.symmachus.org/etc"
scp "$SCRIPT_DIR"/audit.go "$SCRIPT_DIR"/audit_save.go "$SCRIPT_DIR"/audit_status.go "$SCRIPT_DIR"/common.go "$SCRIPT_DIR"/go.mod "$REMOTE_HOST":"$REMOTE_BUILD_DIR"/
ssh "$REMOTE_HOST" "
    set -e
    cd '$REMOTE_BUILD_DIR'
    CGO_ENABLED=1 /usr/local/bin/go build -o audit.cgi audit.go common.go
    CGO_ENABLED=1 /usr/local/bin/go build -o audit-save.cgi audit_save.go common.go
    CGO_ENABLED=1 /usr/local/bin/go build -o audit-status.cgi audit_status.go common.go
    install -m 755 audit.cgi '$REMOTE_CGI_DIR/audit.cgi'
    install -m 755 audit-save.cgi '$REMOTE_CGI_DIR/audit-save.cgi'
    install -m 755 audit-status.cgi '$REMOTE_CGI_DIR/audit-status.cgi'
"
