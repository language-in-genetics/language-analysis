#!/bin/sh

set -eu

HTPASSWD_FILE="${HTPASSWD_FILE:-/var/www/vhosts/lig.symmachus.org/etc/htpasswd}"

if [ "$#" -eq 0 ]; then
    echo "Usage: $0 gregb sally georgina"
    exit 1
fi

mkdir -p "$(dirname "$HTPASSWD_FILE")"

first_user=1
for username in "$@"; do
    if [ "$first_user" -eq 1 ] && [ ! -f "$HTPASSWD_FILE" ]; then
        htpasswd -c "$HTPASSWD_FILE" "$username"
        first_user=0
    else
        htpasswd "$HTPASSWD_FILE" "$username"
    fi
done
