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

The new incremental importer is `bin/crossrefimport`. It imports numbered `*.jsonl.gz` Crossref snapshot files into canonical versioned tables under `public.crossref_*`, and it also has a `-from-raw-text` mode to backfill the existing `public.raw_text_data` corpus into that schema.

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

# Check batch status
./batchcheck.py

# Fetch completed results
./batchfetch.py

# Generate dashboard
./generate_dashboard.py --output-dir ../dashboard

# Run the retracted-vs-control race-language tests directly.
# The focused JSONL gzip avoids slow raw-JSON reads from the large PostgreSQL table.
./retraction_statistics.py \
  --source-jsonl-gz "/dbtemp/March 2026 Public Data File from Crossref/_focused_journals_doi_rebuilt_20260506/focused-journals-doi.jsonl.gz" \
  --output-json ../dashboard/retraction_statistics.json \
  --output-csv ../dashboard/retraction_statistics.csv
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

### Retraction Comparisons

The dashboard pipeline also tests whether focused-journal articles marked as retracted have different race-language vocabulary usage from non-retracted articles. `extractor/retraction_stats.py` classifies Crossref records as retracted research articles, retraction notices, or expression-of-concern update records. Retraction notices are excluded from the case/control test so that update notices are not compared with research articles.

For each vocabulary outcome (`any`, `caucasian`, `white`, `european`, and `other`), the pipeline reports two-sided Fisher exact p-values, Pearson chi-square p-values, rates, risk differences, and Haldane-adjusted odds ratios. Normal dashboard generation writes `retraction_statistics.json` and `retraction_statistics.csv`; the same analysis can be run directly with `extractor/retraction_statistics.py`. On `raksasa`, `cronscript.sh` exports `CROSSREF_RETRACTION_SOURCE_JSONL_GZ` when the focused Crossref compact file is present, so the dashboard uses the compact dump for retraction status and only queries PostgreSQL for processed labels.

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

## Documentation

See [CLAUDE.md](CLAUDE.md) for detailed project documentation, including:
- Complete architecture details
- Database setup and permissions
- Initial import procedures
- Python workflow details
- Configuration options 
