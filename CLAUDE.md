# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a research project focused on analyzing word frequency in academic articles, specifically examining racial/ethnic terminology usage in genetics journals. The project has two main components:

1. **Data Extraction**: Downloads and processes CrossRef database dumps to extract article metadata from specific genetics journals
2. **Content Analysis**: Uses OpenAI's API to analyze article titles and abstracts for racial/ethnic terminology usage

## Architecture

### Go Components (`cmd/` directory)
- **pgjsontool**: Loads JSON data from CrossRef dumps directly into PostgreSQL database (only needed if importing from file-based dumps)

### Python Analysis Pipeline (`extractor/` directory)
- **bulkquery.py**: Creates OpenAI batch jobs to analyze articles for racial terminology using PostgreSQL
- **batchcheck.py**: Monitors the status of OpenAI batch processing jobs
- **batchfetch.py**: Retrieves completed batch results and stores them in PostgreSQL

### Data Storage
- **PostgreSQL**: All data stored in PostgreSQL
  - Article metadata in `public.raw_text_data` table (filesrc column contains JSON text)
  - Batch tracking and analysis results in `languageingenetics` schema (`batches` and `files` tables)

## Database Configuration

### PostgreSQL Setup

The project uses PostgreSQL for all data storage. Database connection uses environment variables (`PGDATABASE`, `PGHOST`, `PGUSER`, etc.) with default `PGDATABASE=crossref`.

**Initial Import (March 2025):**
The initial CrossRef database import was performed using `database/import.sh`, which directly loaded the entire CrossRef database dump into `public.raw_text_data` using PostgreSQL's COPY command. Each row in `raw_text_data` represents a single CrossRef work (article/paper), with the JSON metadata stored as text in the `filesrc` column.

**Future Updates (2026+):**
The `pgjsontool` program needs to be updated to support incremental loading of new CrossRef data without destroying existing records in `public.raw_text_data`. Currently, it is designed for initial bulk imports only.

**Schema Layout:**
- **public schema**: Contains raw CrossRef data
  - `raw_text_data` table: Raw JSON text in `filesrc` column (requires SELECT permission)
- **languageingenetics schema**: Read-write access for processed data
  - `journals` table: List of journals to process with enable/disable flags
  - `files` table: OpenAI analysis results
  - `batches` table: Batch processing tracking

**Required Permissions:**
The `languageingenetics` user needs SELECT permission on `public.raw_text_data`. Run as admin:
```bash
psql -c "GRANT SELECT ON public.raw_text_data TO languageingenetics;"
```

**Database Access:**
```bash
# Connect to PostgreSQL (uses PGDATABASE environment variable)
psql

# View tables
\dt public.raw_text_data
\dt languageingenetics.*

# Manage journals
psql -c "SELECT name, enabled FROM languageingenetics.journals ORDER BY name;"
psql -c "UPDATE languageingenetics.journals SET enabled = false WHERE name = 'Heredity';"
psql -c "INSERT INTO languageingenetics.journals (name) VALUES ('New Journal Name');"
```

**Performance:** For efficient querying on the massive `raw_text_data` table (hundreds of GB), a GIN index is highly recommended:
```sql
-- Run as admin - this will take a long time on a large table
CREATE INDEX idx_raw_text_data_journal
ON public.raw_text_data
USING GIN ((regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'container-title'));
```

See `database/grant_permissions.sql` for the complete setup script.

## Common Development Commands

### Building the Go tools
```bash
make all                    # Build pgjsontool
make bin/pgjsontool        # Build pgjsontool
make clean                 # Remove built binaries
```

Note: `pgjsontool` is only needed if you're importing data from CrossRef dump files. The current workflow reads directly from the existing `public.raw_text_data` table.

### Testing and Linting
```bash
make test                  # Run Go tests
make lint                  # Run golangci-lint (requires installation)
make dev-deps             # Install development dependencies
```

### Running the Data Extraction Pipeline (if needed)

**Note:** The data already exists in `public.raw_text_data`. You only need this if importing new CrossRef dump files.

```bash
# Load articles directly into PostgreSQL
export PGDATABASE=crossref
./bin/pgjsontool -dir "crossref-dump-directory"
```

### Python Analysis Workflow

```bash
cd extractor/

# Set up Python environment (if not already done)
python -m venv .venv
source .venv/bin/activate  # On Linux/Mac
pip install -r ../requirements.txt

# Ensure PGDATABASE is set
export PGDATABASE=crossref

# Run the analysis pipeline
# Process articles from enabled journals in the database
./bulkquery.py --limit 1000

# Or override with specific journal(s)
./bulkquery.py --journal "The American Journal of Human Genetics" --limit 1000

# Can specify multiple journals (bypasses journals table)
./bulkquery.py --journal "The American Journal of Human Genetics" --journal "European Journal of Human Genetics"

# Check batch status and fetch results
./batchcheck.py
./batchfetch.py

# Export results to CSV
psql -c "\copy (SELECT * FROM languageingenetics.files) TO 'ancestry-full.csv' WITH CSV HEADER"
```

## Automation

### Scheduled Processing

The `cronscript.sh` script automates the entire processing and publishing workflow:

```bash
# Run manually
./cronscript.sh

# Schedule with cron (example: run every 6 hours)
crontab -e
# Add line: 0 */6 * * * /home/languageingenetics/Word-Frequency-Analysis-/cronscript.sh
```

The script performs these steps:
1. Fetches completed OpenAI batch results
2. Submits a new batch of articles for processing (default: 4000 articles)
3. Generates static HTML dashboard
4. Syncs dashboard to remote server via rsync

**Configuration** (edit cronscript.sh):
- `BATCH_SIZE`: Number of articles to process per batch (default: 4000)
- `REMOTE_HOST`: Server to sync dashboard to
- `REMOTE_PATH`: Path on remote server

### Dashboard Generation

```bash
cd extractor/

# Generate static dashboard
export PGDATABASE=crossref
./generate_dashboard.py --output-dir ../dashboard

# Dashboard will be created at: dashboard/index.html
```

## Key Configuration

### Target Journals
Journals are managed in the `languageingenetics.journals` PostgreSQL table.

To manage journals:
```sql
-- Disable a journal from processing
UPDATE languageingenetics.journals SET enabled = false WHERE name = 'Heredity';

-- Enable a journal
UPDATE languageingenetics.journals SET enabled = true WHERE name = 'Heredity';

-- Add a new journal
INSERT INTO languageingenetics.journals (name) VALUES ('New Journal Name');

-- List all journals and their status
SELECT name, enabled FROM languageingenetics.journals ORDER BY name;
```

### Database Schema

#### PostgreSQL Tables

**public schema:**
- **raw_text_data**: Contains all CrossRef article data (owned by gregb)
  - `id` (serial primary key)
  - `filesrc` (text) - JSON text containing article metadata
  - Requires SELECT permission for languageingenetics user

**languageingenetics schema:**
- **journals**: Manages which journals are in scope for analysis (created by `pgjsontool`)
  - `id` (serial primary key)
  - `name` (text, unique) - journal name as it appears in CrossRef data
  - `enabled` (boolean, default true) - whether to include this journal in analysis
  - `created_at` (timestamp)
- **files**: Stores OpenAI analysis results per article (created by `bulkquery.py`)
  - `id` (serial primary key)
  - `article_id` (integer) - references raw_text_data(id)
  - `has_abstract`, `pub_year`, `processed`, `batch_id`
  - Analysis results: `caucasian`, `white`, `european`, `european_phrase_used`, `other`, `other_phrase_used`
  - Token usage: `prompt_tokens`, `completion_tokens`
  - `when_processed` (timestamp)
- **batches**: Tracks OpenAI batch jobs (created by `bulkquery.py`)
  - `id` (serial primary key)
  - `openai_batch_id` (text) - OpenAI's batch identifier
  - `when_created`, `when_sent`, `when_retrieved` (timestamps)

## Development Notes

- The Go module is named `crossref-parser` and requires Go 1.21+
- Uses PostgreSQL driver (`github.com/lib/pq`) for database operations
- Python components require OpenAI API access for batch processing
- All scripts use PostgreSQL environment variables for database connections (no connection strings in code)
- The `public.raw_text_data` table is hundreds of GB - ensure the GIN index is created for performance