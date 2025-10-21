#!/usr/bin/env python3

import argparse
import json
import os
import openai
import sys
import tempfile
from collections import Counter
from datetime import datetime
import psycopg2
import psycopg2.extras

parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, help="Stop after processing this many articles")
parser.add_argument("--journal", action="append", help="Filter by journal name (can specify multiple times, e.g., --journal 'The American Journal of Human Genetics')")
parser.add_argument("--output-file", help="Where to put the batch file (default: random tempfile)")
parser.add_argument("--dry-run", action="store_true", help="Don't send the batch to OpenAI")
parser.add_argument("--batch-id-save-file", help="What file to put the local batch ID into")
parser.add_argument("--openai-api-key", default=os.path.expanduser("~/.openai.key"))
parser.add_argument("--explain-queries", action="store_true", help="Run EXPLAIN on all queries and log to file")
parser.add_argument("--explain-log", default="bulkquery_explains.log", help="Log file for EXPLAIN output")
args = parser.parse_args()

# PostgreSQL connection will use environment variables:
# PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
# or use defaults from ~/.pgpass or pg_service.conf

# Create output file if not specified
if args.output_file is None:
    tf = tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.jsonl')
    args.output_file = tf.name
    tf.close()

# Connect to PostgreSQL using environment variables
conn = psycopg2.connect("")
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Helper function to execute queries with optional EXPLAIN logging
def execute_query(cursor_to_use, sql, params=None):
    """Execute a query, optionally logging EXPLAIN output"""
    if args.explain_queries:
        explain_cursor = conn.cursor()
        try:
            if params:
                explain_cursor.execute("EXPLAIN (ANALYZE, BUFFERS, VERBOSE) " + sql, params)
            else:
                explain_cursor.execute("EXPLAIN (ANALYZE, BUFFERS, VERBOSE) " + sql)
            explain_output = "\n".join(row[0] for row in explain_cursor.fetchall())

            with open(args.explain_log, 'a') as f:
                f.write(f"\n{'='*80}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Query:\n{sql}\n")
                if params:
                    f.write(f"Parameters: {params}\n")
                f.write(f"\nEXPLAIN output:\n{explain_output}\n")
        finally:
            explain_cursor.close()

    # Execute the actual query
    if params:
        cursor_to_use.execute(sql, params)
    else:
        cursor_to_use.execute(sql)
    return cursor_to_use

# Set search path
cursor.execute("SET search_path TO languageingenetics, public")

# Initialize explain log if needed
if args.explain_queries:
    with open(args.explain_log, 'w') as f:
        f.write(f"Query Explanation Log - Generated {datetime.now().isoformat()}\n")
        f.write(f"{'='*80}\n")

# Ensure diagnostics table exists so we can capture per-article reasons
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS languageingenetics.batch_diagnostics (
        id BIGSERIAL PRIMARY KEY,
        batch_id INT NOT NULL REFERENCES languageingenetics.batches(id) ON DELETE CASCADE,
        article_id INT,
        event_type TEXT NOT NULL,
        details JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """
)
cursor.execute(
    """
    CREATE INDEX IF NOT EXISTS batch_diagnostics_batch_id_idx
        ON languageingenetics.batch_diagnostics (batch_id)
    """
)

# Diagnostics helpers -----------------------------------------------------
stats = Counter()


# Start a transaction for this batch
cursor.execute("BEGIN")
cursor.execute("INSERT INTO languageingenetics.batches DEFAULT VALUES RETURNING id")
batch_id = cursor.fetchone()['id']


def record_diagnostic(event_type, article_id, **details):
    """Persist a diagnostic event for later analysis."""
    cursor.execute(
        """
        INSERT INTO languageingenetics.batch_diagnostics (batch_id, article_id, event_type, details)
        VALUES (%s, %s, %s, %s)
        """,
        [batch_id, article_id, event_type, psycopg2.extras.Json(details) if details else None]
    )

# Define the tool schema for OpenAI
tools = [{
    "type": "function",
    "function": {
        "name": "analyze_text",
        "description": "Analyze text for racial/ethnic terminology",
        "parameters": {
            "type": "object",
            "properties": {
                "caucasian": {
                    "type": "boolean",
                    "description": "uses the word Caucasian, or similar"
                },
                "white": {
                    "type": "boolean",
                    "description": "uses the word 'white' to refer to race, ancestry, ethnicity, population or equivalent"
                },
                "european": {
                    "type": "boolean",
                    "description": "uses a phrase like 'European ancestry'"
                },
                "european_phrase_used": {
                    "type": "string",
                    "description": "the actual phrase used if european is true, blank otherwise"
                },
                "other": {
                    "type": "boolean",
                    "description": "uses some other phrase to describe someone with European/Caucasian/white ancestry, race, ethnicity or population"
                },
                "other_phrase_used": {
                    "type": "string",
                    "description": "what phrase was used if 'other' is true, blank otherwise"
                }
            },
            "required": ["caucasian", "white", "european", "european_phrase_used", "other", "other_phrase_used"]
        }
    }
}]

def process_article(article_id, metadata):
    stats['examined'] += 1

    if metadata is None:
        stats['missing_metadata'] += 1
        record_diagnostic('skipped', article_id, reason='missing_metadata')
        return False

    # Check if article is already processed
    execute_query(cursor, "SELECT id FROM languageingenetics.files WHERE article_id = %s", [article_id])
    if cursor.fetchone() is not None:
        stats['already_processed'] += 1
        record_diagnostic('skipped', article_id, reason='already_processed')
        return False

    # Extract information
    title = metadata.get('title', [None])[0] if isinstance(metadata.get('title'), list) else metadata.get('title')
    if not title:
        stats['missing_title'] += 1
        record_diagnostic('skipped', article_id, reason='missing_title')
        print(f"Warning: No title found in article {article_id}", file=sys.stderr)
        return False

    abstract = None
    if 'abstract' in metadata:
        abstract = metadata['abstract']

    # Get publication year
    pub_year = None
    if 'published' in metadata and 'date-parts' in metadata['published']:
        try:
            pub_year = metadata['published']['date-parts'][0][0]
        except (IndexError, TypeError):
            pass

    # Create the prompt
    prompt = "Does this article use any terms like \"Caucasian\" or \"white\" or \"European ancestry\" in a way that refers to race, ancestry, ethnicity or population?\n\n"
    prompt += f"TITLE: {title}\n"
    if abstract:
        prompt += f"ABSTRACT: {abstract}\n"

    # Create the batch request
    batch_text = {
        "custom_id": str(article_id),
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-5-mini",
            "messages": [{"role": "user", "content": prompt}],
            "tools": tools,
            "tool_choice": {"type": "function", "function": {"name": "analyze_text"}}
        }
    }

    # Write to batch file
    with open(args.output_file, 'a') as f:
        f.write(json.dumps(batch_text) + "\n")

    # Add to database
    cursor.execute(
        "INSERT INTO languageingenetics.files (article_id, has_abstract, pub_year, batch_id) VALUES (%s, %s, %s, %s)",
        [article_id, abstract is not None, pub_year, batch_id]
    )

    stats['submitted'] += 1
    record_diagnostic('submitted', article_id, has_abstract=abstract is not None, pub_year=pub_year)
    return True

# Determine which journals to query
journals_to_query = []
if args.journal:
    # Use explicitly specified journals
    journals_to_query = args.journal
else:
    # Query enabled journals from the database
    execute_query(cursor, "SELECT name FROM languageingenetics.journals WHERE enabled = true")
    journals_to_query = [row['name'] for row in cursor]
    if journals_to_query:
        print(f"Processing {len(journals_to_query)} enabled journals from database", file=sys.stderr)

# Build query to get articles from raw_text_data
# Parse the filesrc JSON text column
# Use UNION ALL instead of OR to allow GIN index usage (see bulkquery_optimization_suggestions.md)
query_params = []

if journals_to_query:
    # Build UNION ALL query - each journal gets its own SELECT that can use the GIN index
    subqueries = []
    for journal in journals_to_query:
        subqueries.append("""
        SELECT
            id,
            regexp_replace(
                regexp_replace(filesrc, E'\n', ' ', 'g'),
                E'\t', '    ', 'g'
            )::jsonb as data
        FROM public.raw_text_data
        WHERE regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title' @> %s::jsonb
        """)
        query_params.append(json.dumps([journal]))

    query = "SELECT * FROM (\n" + "\n    UNION ALL\n".join(subqueries) + "\n) AS combined"

    if args.limit:
        query += f" LIMIT {args.limit}"
else:
    # No journal filter - select all articles (shouldn't happen in normal operation)
    query = """
    SELECT
        id,
        regexp_replace(
            regexp_replace(filesrc, E'\n', ' ', 'g'),
            E'\t', '    ', 'g'
        )::jsonb as data
    FROM public.raw_text_data
    """
    if args.limit:
        query += f" LIMIT {args.limit}"

# Create a separate cursor for fetching articles
article_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
execute_query(article_cursor, query, query_params)

# Process articles
processed_count = 0
for row in article_cursor:
    article_id = row['id']
    metadata = row['data']

    if process_article(article_id, metadata):
        processed_count += 1
        if processed_count % 100 == 0:
            print(f"Processed {processed_count} articles...", file=sys.stderr)

article_cursor.close()

if processed_count == 0:
    print("No files were processed", file=sys.stderr)
    conn.rollback()
    conn.close()
    sys.exit(1)

print("Batch preparation diagnostics:", file=sys.stderr)
print(f"  Articles examined: {stats['examined']}", file=sys.stderr)
print(f"  Submitted to batch: {stats['submitted']}", file=sys.stderr)
print(f"  Skipped (already processed): {stats['already_processed']}", file=sys.stderr)
print(f"  Skipped (missing title): {stats['missing_title']}", file=sys.stderr)
if stats['missing_metadata']:
    print(f"  Skipped (missing metadata): {stats['missing_metadata']}", file=sys.stderr)

record_diagnostic(
    'summary',
    None,
    totals={key: int(value) for key, value in stats.items()}
)

if args.dry_run:
    conn.rollback()
    conn.close()
    sys.exit(0)

# Submit the batch to OpenAI
api_key = open(args.openai_api_key).read().strip()
client = openai.OpenAI(api_key=api_key)

batch_input_file = client.files.create(
    file=open(args.output_file, "rb"),
    purpose="batch"
)

result = client.batches.create(
    input_file_id=batch_input_file.id,
    endpoint="/v1/chat/completions",
    completion_window="24h",
    metadata={
        "description": f"batch {batch_id} (wordfreq)",
        "local_batch_id": f"{batch_id}"
    }
)

cursor.execute(
    "UPDATE languageingenetics.batches SET openai_batch_id = %s, when_sent = CURRENT_TIMESTAMP WHERE id = %s",
    [result.id, batch_id]
)

if cursor.rowcount != 1:
    conn.rollback()
    conn.close()
    sys.exit(f"Unexpectedly updated {cursor.rowcount} rows when we set the openai_batch id to {result.id} for batch {batch_id}")

conn.commit()
conn.close()

if args.explain_queries:
    print(f"Query explanations written to {args.explain_log}", file=sys.stderr)

if args.batch_id_save_file:
    with open(args.batch_id_save_file, 'w') as bisf:
        bisf.write(f"{batch_id}")

