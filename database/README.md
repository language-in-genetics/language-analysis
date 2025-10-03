# Database Setup and Configuration

## Overview

This project uses PostgreSQL exclusively for all data storage, including article metadata, batch tracking, and OpenAI analysis results.

## PostgreSQL Configuration

**Database:** `crossref` (set via `PGDATABASE` environment variable)

### Schema Access

- **public schema**: Contains raw CrossRef data in `raw_text_data` table (requires SELECT permission)
- **languageingenetics schema**: Read-write access for project data

### Tables

#### public.raw_text_data
Contains all CrossRef article data as JSON text. This is a massive table (hundreds of GB).

```sql
-- Table owned by gregb
CREATE TABLE public.raw_text_data (
    id SERIAL PRIMARY KEY,
    filesrc TEXT  -- Raw JSON text of article metadata
);
```

**Required Permission:**
```sql
-- Run as admin (gregb)
GRANT SELECT ON public.raw_text_data TO languageingenetics;
```

**Critical Index for Performance:**
This index is essential for efficient querying by journal. Without it, queries will be extremely slow:
```sql
-- Run as admin - this will take a long time on a large table
CREATE INDEX idx_raw_text_data_journal
ON public.raw_text_data
USING GIN ((regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'container-title'));
```

#### languageingenetics.journals
Manages which journals to include in analysis.

```sql
CREATE TABLE languageingenetics.journals (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

### Common Operations

```bash
# Connect to database
psql

# View schema structure
\dt languageingenetics.*

# List all journals
psql -c "SELECT name, enabled FROM languageingenetics.journals ORDER BY name;"

# Disable a journal
psql -c "UPDATE languageingenetics.journals SET enabled = false WHERE name = 'Heredity';"

# Add a new journal
psql -c "INSERT INTO languageingenetics.journals (name) VALUES ('New Journal Name');"

# Count articles
psql -c "SELECT COUNT(*) FROM languageingenetics.articles;"

# Count articles by journal
psql -c "SELECT data->'container-title'->0 as journal, COUNT(*) FROM languageingenetics.articles GROUP BY journal ORDER BY COUNT(*) DESC LIMIT 10;"
```

## Current Journals

The following journals are currently configured:

1. European Journal of Human Genetics
2. Familial Cancer
3. Genetic Epidemiology
4. Heredity
5. Human Genetics
6. Human Genomics
7. Journal of Community Genetics

All journals are enabled by default. Use SQL commands to enable/disable specific journals.

#### languageingenetics.files
Stores OpenAI analysis results per article.

```sql
CREATE TABLE languageingenetics.files (
    id SERIAL PRIMARY KEY,
    article_id INTEGER UNIQUE REFERENCES languageingenetics.articles(id),
    has_abstract BOOLEAN,
    pub_year INTEGER,
    processed BOOLEAN DEFAULT false,
    batch_id INTEGER REFERENCES languageingenetics.batches(id),
    when_processed TIMESTAMP,
    caucasian BOOLEAN,
    white BOOLEAN,
    european BOOLEAN,
    european_phrase_used TEXT,
    other BOOLEAN,
    other_phrase_used TEXT,
    prompt_tokens INTEGER,
    completion_tokens INTEGER
);
```

#### languageingenetics.batches
Tracks OpenAI batch jobs.

```sql
CREATE TABLE languageingenetics.batches (
    id SERIAL PRIMARY KEY,
    openai_batch_id TEXT,
    when_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    when_sent TIMESTAMP,
    when_retrieved TIMESTAMP
);
```

## Initial Setup

### Admin Tasks (run as gregb)

1. **Grant permissions** - Run `database/grant_permissions.sql`:
   ```bash
   psql -f database/grant_permissions.sql
   ```
   This grants SELECT on `raw_text_data` and creates the critical GIN index.

2. **Create index** - The index creation will take a long time on a large table. Monitor progress:
   ```sql
   SELECT phase, blocks_done, blocks_total,
          round(100.0 * blocks_done / nullif(blocks_total, 0), 1) AS percent_done
   FROM pg_stat_progress_create_index;
   ```

### Application Setup (run as languageingenetics)

The application automatically creates its own tables:

1. **bulkquery.py** creates the `files` and `batches` tables on first run
2. **pgjsontool** creates the `journals` table (only if importing from CrossRef dump files)
3. Populate the journals table manually:
   ```sql
   INSERT INTO languageingenetics.journals (name) VALUES
       ('The American Journal of Human Genetics'),
       ('European Journal of Human Genetics'),
       ('Human Genetics'),
       ('Heredity');
   ```

## Backup and Maintenance

```bash
# Backup PostgreSQL data
pg_dump crossref -n languageingenetics > backup-$(date +%Y%m%d).sql

# Restore PostgreSQL
psql crossref < backup-20250101.sql

# Analyze PostgreSQL tables for query optimization
psql -c "ANALYZE languageingenetics.articles;"
psql -c "ANALYZE languageingenetics.journals;"
psql -c "ANALYZE languageingenetics.files;"
psql -c "ANALYZE languageingenetics.batches;"
```

## Performance Tuning

The GIN indexes on the articles table enable efficient queries for:
- Filtering articles by journal name
- JSONB field queries

To ensure optimal performance:

```sql
-- Update statistics
ANALYZE languageingenetics.articles;

-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan
FROM pg_stat_user_indexes
WHERE schemaname = 'languageingenetics';

-- Vacuum if needed
VACUUM ANALYZE languageingenetics.articles;
```

---

## Legacy: Alternate plan (old method)

Because the data seems so useful, here's a way of putting it into a postgresql database.

1. Create a database in postgresql. I've used crossref as the database name. `createdb crossref`

2. Download the crossref data via bittorrent (same as step 1 in the normal plan in `../README.md` )

3. In postgresql run `create table raw_text_data (id serial primary key, filesrc text);`

4. Run `import.sh` (at the moment this is Linux/OSX only) from the directory where the crossref
data is. Wait many hours.

5. Run `schema.sql`
