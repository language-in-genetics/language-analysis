# Human Audit Workflow

This repo now includes a `merah`-style human-audit workflow for `lig.symmachus.org`:

- PostgreSQL on `raksasa` stores the canonical audit batch definitions and imported review results.
- SQLite on `merah` stores the live review state used by the CGI interface.
- The public status page is intended to read from SQLite on `merah`, so it reflects review changes immediately.
- The nightly import copies that SQLite database back into PostgreSQL for canonical reporting and downstream analysis.

## Main Pieces

- [database/audit_schema.sql](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/database/audit_schema.sql)
  - PostgreSQL tables and the `audit_article_status_view`.
- [extractor/create_audit_batch.py](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/extractor/create_audit_batch.py)
  - Creates a reproducible positive/negative sample batch in PostgreSQL and seeds the local SQLite DB copy.
- [extractor/import_audit_reviews.py](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/extractor/import_audit_reviews.py)
  - Imports the `merah` SQLite review state back into PostgreSQL.
- [audit/sync_audit_db.sh](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/audit/sync_audit_db.sh)
  - Pulls `/var/www/vhosts/lig.symmachus.org/db/lig_audit.db` from `merah`.
- [audit_cgi](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/audit_cgi)
  - The Go CGI programs and deployment notes for the live audit UI.

## Reporting Split

The intended split is:

- `merah` SQLite for the live review UI and the public audit-status pages.
- PostgreSQL for canonical reporting, dashboard summaries, and anything cross-host or analysis-heavy.

That keeps the reviewer-facing pages immediate without making the main project reporting depend on a live CGI-hosted SQLite query path.
