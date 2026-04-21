# LIG Audit CGI

This directory contains the `lig.symmachus.org` human-audit CGI programs.

## Programs

- `audit.cgi`
  - Authenticated review interface.
- `audit-save.cgi`
  - Authenticated save handler.
- `audit-status.cgi`
  - Public read-only audit-status page showing reviewed and pending items.

## Runtime Layout On `merah`

- CGI binaries: `/var/www/vhosts/lig.symmachus.org/cgi-bin/`
- SQLite DB: `/var/www/vhosts/lig.symmachus.org/db/lig_audit.db`
- htpasswd file: `/var/www/vhosts/lig.symmachus.org/etc/htpasswd`
- static dashboard: `/var/www/vhosts/lig.symmachus.org/htdocs/`

## Authentication Pattern

This follows the Stephanos review pattern:

- OpenBSD `httpd` applies HTTP Basic Auth to the protected CGI programs.
- The CGI reads the authenticated username from `REMOTE_USER`.
- `audit-status.cgi` stays public.
- `/db/*` should be blocked from web access.

See [lig-audit.conf](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/audit_cgi/lig-audit.conf).

## Build

Build natively on the target host because the SQLite driver uses CGO:

```bash
cd /var/www/vhosts/lig.symmachus.org/cgi-bin
CGO_ENABLED=1 go build -o audit.cgi audit.go common.go
CGO_ENABLED=1 go build -o audit-save.cgi audit_save.go common.go
CGO_ENABLED=1 go build -o audit-status.cgi audit_status.go common.go
chmod 755 audit.cgi audit-save.cgi audit-status.cgi
```

Local build is also useful for syntax checking on macOS:

```bash
cd audit_cgi
make
```
