# Word Frequency Analysis - Language in Genetics

A research project analyzing racial/ethnic terminology usage in genetics journals. The system processes CrossRef article metadata and uses OpenAI's API to identify terminology patterns in academic publications.

## Architecture

- **PostgreSQL Database**: Stores CrossRef article metadata and analysis results
- **Go Tools** (`cmd/pgjsontool`, `cmd/crossrefimport`): Legacy bulk loader plus incremental importer
- **Python Pipeline** (`extractor/`): OpenAI batch processing for content analysis
- **Automated Dashboard**: Generates static HTML dashboards showing analysis results
- **Human Audit Workflow** (`audit/`, `audit_cgi/`): authenticated review UI on `merah`, public audit-status pages, and nightly SQLite-to-PostgreSQL import

## Import Status

The current `database/import.sh` path is the legacy one-time bulk loader used for the initial March 2025 ingest. It is not the right shape for yearly Crossref refreshes, because annual dumps are full snapshots and the downstream analysis tables currently depend on `raw_text_data.id`.

The redesign plan for incremental yearly imports is documented in [database/incremental_import_redesign.md](database/incremental_import_redesign.md).

The new incremental importer is `bin/crossrefimport`. It imports a SQLite staging database into canonical versioned tables under `public.crossref_*`, and it also has a `-from-raw-text` mode to backfill the existing `public.raw_text_data` corpus into that schema.

For full annual snapshots after the 2025 backfill, prefilter the dump before
importing. Build a SQLite DOI cache, compute missing legacy text fingerprints in
that cache, classify the dump into a SQLite stage with `new` and `changed`
records, then import only those staged categories. This avoids rewriting legacy
PostgreSQL rows just to fill `text_fingerprint` and avoids row-by-row
PostgreSQL lookups while streaming the dump. See
[database/crossref_sqlite_cache_pipeline.md](database/crossref_sqlite_cache_pipeline.md).

## Quick Start

### Prerequisites

- PostgreSQL with the CrossRef database imported
- Python 3.x with virtual environment
- OpenAI API key
- Go 1.21+ (only if importing new CrossRef data)

### Setup

```bash
# Set database environment variable
export PGDATABASE=crossref

# Set up Python environment
cd extractor/
python -m venv .venv
source .venv/bin/activate
pip install -r ../requirements.txt
```

### Running Analysis

```bash
cd extractor/
# Process articles from enabled journals
./bulkquery.py --limit 1000

# Process the title/abstract Homo sapiens filter from enabled journals
./human_subject_bulkquery.py --limit 1000

# Check batch status
./batchcheck.py

# Fetch completed results
./batchfetch.py

# Fetch completed Homo sapiens filter results
./human_subject_batchfetch.py

# Generate dashboard
./generate_dashboard.py --output-dir ../dashboard

# Run the retracted-vs-control race-language tests directly.
# The focused SQLite stage avoids slow raw-JSON reads from the large PostgreSQL table.
./retraction_statistics.py \
  --source-sqlite "/dbtemp/March 2026 Public Data File from Crossref/_focused_journals_doi/focused-journals.sqlite" \
  --output-json ../dashboard/retraction_statistics.json \
  --output-csv ../dashboard/retraction_statistics.csv \
  --output-html ../dashboard/retraction_statistics.html
```

## Computational Analysis Methodology

The `extractor/bulkquery.py` script implements an automated approach to analyze genetics articles for specific racial terminology. It processes metadata.json files containing article titles and abstracts, then submits them to OpenAI's API for analysis.

The core of the analysis uses a carefully constructed prompt:

```text
Does this article use any terms like "Caucasian" or "white" or "European ancestry" in a way that refers to race, ancestry, ethnicity or population?

TITLE: {title}
ABSTRACT: {abstract}
```

This prompt is deliberately framed in a neutral manner to avoid biasing the language model's analysis. It specifically asks about terms related to European ancestry without suggesting preference for any particular terminology.

The analysis is structured through a function-calling API that forces OpenAI to return standardized responses across all articles. The analysis function includes parameters for detecting:
- "caucasian" terminology
- "white" racial descriptors
- "European ancestry" phrasing
- Other phrases describing European populations

When phrases are detected, the system also captures the exact terminology used, enabling detailed analysis of language variations across the literature.

The batch processing system allows efficient processing of thousands of articles with proper error handling and progress tracking, making large-scale analysis feasible within reasonable time and cost constraints.

### Homo Sapiens Filter

The title/abstract Homo sapiens filter is a separate OpenAI batch pass. Run
`extractor/human_subject_bulkquery.py` to submit current focused-journal works
that do not yet have a row in `languageingenetics.human_subject_classifications`.
The default model is `gpt-5.4-mini`.

The classifier asks whether the paper is about humans, Homo sapiens, based only
on title and abstract. It stores `about_humans`, short evidence, confidence,
model, token usage, and batch metadata. Fetch completed results with
`extractor/human_subject_batchfetch.py`. `extractor/batchcheck.py` reports both
terminology batches and Homo sapiens filter batches; `extractor/batchfetch.py`
only consumes terminology batches.

`cronscript.sh` runs this pass automatically on `raksasa`. Set
`HUMAN_SUBJECT_BATCH_SIZE` to override the number submitted per cron run, or to
`0` to skip new Homo sapiens submissions while still fetching completed batches.

### Retraction Comparisons

The dashboard pipeline also tests whether focused-journal articles marked as retracted have different race-language vocabulary usage from non-retracted articles. `extractor/retraction_stats.py` classifies Crossref records as retracted research articles, retraction notices, or expression-of-concern update records. Retraction notices are excluded from the case/control test so that update notices are not compared with research articles.

For each vocabulary outcome (`any`, `caucasian`, `white`, `european`, and `other`), the pipeline reports two-sided Fisher exact p-values, Pearson chi-square p-values, rates, risk differences, and Haldane-adjusted odds ratios. Normal dashboard generation writes `retraction_statistics.html`, `retraction_statistics.json`, and `retraction_statistics.csv`; the same analysis can be run directly with `extractor/retraction_statistics.py`. On `raksasa`, `cronscript.sh` exports `CROSSREF_RETRACTION_SOURCE_SQLITE` when the focused Crossref SQLite stage is present, so the dashboard uses the compact stage for retraction status and only queries PostgreSQL for processed labels.

### Automation

The `cronscript.sh` script automates the entire workflow:

```bash
# Run manually
./cronscript.sh

# Schedule with cron (every 6 hours)
crontab -e
# Add: 0 */6 * * * /home/languageingenetics/Word-Frequency-Analysis-/cronscript.sh
```

## Database Schema

**public.raw_text_data**: CrossRef article metadata (JSON in `filesrc` column)

**languageingenetics schema**:
- `journals`: Manages which journals to process
- `files`: Analysis results per article
- `batches`: OpenAI batch job tracking
- `human_subject_classifications`: Title/abstract Homo sapiens filter results

## Managing Journals

```sql
-- List journals and their status
SELECT name, enabled FROM languageingenetics.journals ORDER BY name;

-- Disable a journal
UPDATE languageingenetics.journals SET enabled = false WHERE name = 'Heredity';

-- Add a new journal
INSERT INTO languageingenetics.journals (name) VALUES ('New Journal Name');
```

## Development

```bash
# Build Go tools
make all

# Build only the incremental importer
make bin/crossrefimport

# Build the full annual prefilter pipeline tools
make bin/crossrefcachebuild bin/crossrefclassify bin/crossrefimport bin/crossrefstagefromjsonl

# Classify a full Crossref dump through a SQLite DOI cache.
# This does not import unless RUN_IMPORT=1 is set.
database/run_crossref_prefilter_pipeline.sh

# Run tests
make test

# Run linter
make lint
```

## Human Audit

The audit subsystem samples explicit classifier-label buckets (`caucasian`, `white`, `european`, `other`) plus a `none of these labels` control bucket for human review.

- Create a reproducible sample batch:
  - `cd extractor && uv run create_audit_batch.py --matched-label-size 100 --none-size 100`
  - The default split is `25` each for `caucasian`, `white`, `european`, and `other`, plus `100` `none_of_these_labels` controls.
- Pull the live review SQLite database from `merah`:
  - `./audit/sync_audit_db.sh`
- Import the SQLite review state into PostgreSQL:
  - `cd extractor && uv run import_audit_reviews.py --sqlite-db ../audit/review_data/lig_audit.db`

The intended split is:

- live review UI and public audit status on `merah` SQLite
- canonical reporting and dashboard summaries in PostgreSQL

### Homo Sapiens Audit

The Homo sapiens title/abstract classifier has a parallel audit lane. It samples
processed `languageingenetics.human_subject_classifications` rows into the same
SQLite review database, but uses separate `human_subject_audit_*` tables so the
race-language audit remains unchanged.

- Create a balanced reproducible sample:
  - `cd extractor && uv run create_human_subject_audit_batch.py --sample-size 200 --batch-slug human-subject-2026-seed42 --notes "Title/abstract Homo sapiens classifier validation"`
- Push the seeded SQLite database to `merah`:
  - `./audit/push_audit_db.sh`
- Review UI:
  - `/cgi-bin/audit-human-subject.cgi` requires login
  - `/cgi-bin/audit-human-subject-status.cgi` uses the same audit login
- Import reviews back into PostgreSQL:
  - `cd extractor && uv run import_human_subject_audit_reviews.py --sqlite-db ../audit/review_data/lig_audit.db`

Reviewers label whether the paper is about humans from title and abstract. The
system derives `correct`, `false_positive`, and `false_negative` outcomes by
comparing that human label with the bot's `about_humans` value.

### Full-Text AI Processing

The same `merah` SQLite review database also supports a full-article AI processing track. The human task is to upload or paste full papers; the `raksasa` cron job pulls those uploads, extracts text when needed, and sends full article text to AI for terminology analysis.

- Create a reproducible 100-article batch through the end of 2025:
  - `cd extractor && uv run create_fulltext_audit_batch.py --sample-size 100 --max-year 2025 --batch-slug fulltext-2025-seed42 --notes "Full-article validation sample through 2025"`
- Upload UI:
  - `/cgi-bin/fulltext-upload.cgi` requires login
  - `/cgi-bin/fulltext-verify.cgi` is the authenticated queue browser
  - `/cgi-bin/fulltext-status.cgi` is public status
- Process queued uploads after syncing the `merah` SQLite database backup:
  - `cd extractor && uv run process_fulltext_analysis.py --sqlite-db ../audit/review_data/lig_audit.db --limit 10`
- Import the SQLite upload and AI state into PostgreSQL:
  - `cd extractor && uv run import_fulltext_audit_reviews.py --sqlite-db ../audit/review_data/lig_audit.db`
- Push a newly seeded local SQLite database to `merah`:
  - `./audit/sync_audit_db.sh`
  - `cd extractor && uv run create_fulltext_audit_batch.py --sample-size 100 --max-year 2025 --batch-slug fulltext-2025-seed42`
  - `./audit/push_audit_db.sh`

The full-text tables track article selection, full-text acquisition status, uploaded PDF/text/HTML blobs in SQLite, extracted text, AI analysis status, AI terminology flags, and upload notes. The normal `cronscript.sh` syncs a backup of the `merah` SQLite database back to `raksasa`, processes queued uploads from the copied database, and imports both title/abstract audit state and full-text AI state into PostgreSQL.

### CGI Deployment

CGI deployment is handled by GitHub Actions in `.github/workflows/deploy-lig-audit-cgi.yml`. The workflow SSHes to `merah`, copies the `audit_cgi/` source, builds the Go CGI binaries on `merah`, installs them under `/var/www/vhosts/lig.symmachus.org/cgi-bin`, and applies the SQLite schema migrations.

Required GitHub secrets:

- `MERAH_SSH_HOST`
- `MERAH_SSH_USER`
- `MERAH_SSH_PRIVATE_KEY`
- `MERAH_SSH_PORT` (optional; defaults to `22`)

## Documentation

See [CLAUDE.md](CLAUDE.md) for detailed project documentation, including:
- Complete architecture details
- Database setup and permissions
- Initial import procedures
- Python workflow details
- Configuration options 
