# Human Audit Workflow

This repo now includes a `merah`-style human-audit workflow for `lig.symmachus.org`:

- PostgreSQL on `raksasa` stores the canonical audit batch definitions and imported review results.
- SQLite on `merah` stores the live review state used by the CGI interface.
- The public status pages read from SQLite on `merah`, so they reflect review, upload, and AI-processing changes immediately.
- The nightly import copies that SQLite database back into PostgreSQL for canonical reporting and downstream analysis.

## Main Pieces

- [database/audit_schema.sql](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/database/audit_schema.sql)
  - PostgreSQL tables and the `audit_article_status_view`.
- [extractor/create_audit_batch.py](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/extractor/create_audit_batch.py)
  - Creates a reproducible label-specific audit batch in PostgreSQL and seeds the local SQLite DB copy.
- [extractor/import_audit_reviews.py](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/extractor/import_audit_reviews.py)
  - Imports the `merah` SQLite review state back into PostgreSQL.
- [audit/sync_audit_db.sh](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/audit/sync_audit_db.sh)
  - Pulls `/var/www/vhosts/lig.symmachus.org/db/lig_audit.db` from `merah`.
- [audit_cgi](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/audit_cgi)
  - The Go CGI programs and deployment notes for the live audit UI.
  - Also contains the full-text upload and AI-processing CGI programs (`fulltext-upload.cgi`, `fulltext-verify.cgi`, and `fulltext-status.cgi`).

## Reporting Split

The intended split is:

- `merah` SQLite for the live review UI and the public audit-status pages.
- `merah` CGI for human full-paper uploads into the full-text AI queue.
- `raksasa` cron for pulling uploaded files, extracting text, and running AI analysis.
- PostgreSQL for canonical reporting, dashboard summaries, and anything cross-host or analysis-heavy.

That keeps the reviewer-facing pages immediate without making the main project reporting depend on a live CGI-hosted SQLite query path.

## Current Target Labels

The audit batch generator and reviewer UI use the same target labels throughout:

- `caucasian`
- `white`
- `european`
- `other`
- `none_of_these_labels`

## Full-Text AI Processing Track

The full-text track uses the same SQLite database on `merah`, but the human task is upload, not manual terminology coding. Humans upload or paste a full journal article, and the `raksasa` cron job extracts text where needed and sends the full article text to AI for terminology analysis.

- `fulltext_batches`: reproducible full-article sample batches.
- `fulltext_articles`: article metadata, full-text acquisition status, uploaded file path, extracted text, AI analysis status, AI terminology flags, and upload notes.

The corresponding canonical PostgreSQL tables are created by [database/audit_schema.sql](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/database/audit_schema.sql):

- `languageingenetics.fulltext_audit_batches`
- `languageingenetics.fulltext_audit_articles`
- `languageingenetics.fulltext_audit_reviews`
- `languageingenetics.fulltext_audit_status_view`

Useful commands:

```bash
cd extractor
uv run create_fulltext_audit_batch.py --sample-size 100 --max-year 2025 --batch-slug fulltext-2025-seed42
uv run import_fulltext_audit_reviews.py --sqlite-db ../audit/review_data/lig_audit.db
uv run process_fulltext_analysis.py --sqlite-db ../audit/review_data/lig_audit.db --limit 10
```

When seeding a new live batch, first pull the current live database, then create
the batch locally, then push it back:

```bash
./audit/sync_audit_db.sh
cd extractor
uv run create_fulltext_audit_batch.py --sample-size 100 --max-year 2025 --batch-slug fulltext-2025-seed42
cd ..
./audit/push_audit_db.sh
```

The nightly `raksasa` cron path pulls a SQLite backup and uploaded full-text files from `merah`, processes queued full-text uploads, then imports both the title/abstract audit and the full-text AI state into PostgreSQL.
