# Quick Setup Guide

## Prerequisites

1. **Database Setup** (requires admin access):
   ```bash
   # Run as gregb
   psql -f database/grant_permissions.sql
   ```
   This will:
   - Grant SELECT permission on `public.raw_text_data` to `languageingenetics` user
   - Create a GIN index for efficient journal filtering (takes a long time on large tables)

2. **Environment Variables**:
   ```bash
   export PGDATABASE=crossref
   # Other PostgreSQL env vars (PGHOST, PGUSER, etc.) as needed
   ```

## Running the Analysis Pipeline

```bash
cd extractor/

# Submit a batch of articles for processing
./bulkquery.py --limit 1000

# Check batch status
./batchcheck.py

# Fetch completed results
./batchfetch.py --report-costs

# Generate dashboard
./generate_dashboard.py --output-dir ../dashboard
```

## Automated Processing

The `cronscript.sh` handles the full workflow:
```bash
# Run manually
./cronscript.sh

# Or schedule with cron (every 6 hours)
0 */6 * * * /home/languageingenetics/Word-Frequency-Analysis-/cronscript.sh
```

## Managing Journals

```bash
# List all journals
psql -c "SELECT name, enabled FROM languageingenetics.journals ORDER BY name;"

# Disable a journal
psql -c "UPDATE languageingenetics.journals SET enabled = false WHERE name = 'Heredity';"

# Add a new journal
psql -c "INSERT INTO languageingenetics.journals (name) VALUES ('New Journal Name');"
```

## Data Schema

- **public.raw_text_data**: Contains all CrossRef articles (JSON text in `filesrc` column)
- **languageingenetics.journals**: Manages which journals to process
- **languageingenetics.files**: Stores OpenAI analysis results
- **languageingenetics.batches**: Tracks batch processing jobs

## Important Notes

- The `raw_text_data` table is hundreds of GB - the GIN index is **critical** for performance
- All scripts use environment variables for database connection (no `--pg-conn` parameter)
- The `languageingenetics` user needs SELECT permission on `public.raw_text_data`
- See `CLAUDE.md` for detailed documentation
