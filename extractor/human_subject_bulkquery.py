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


BATCH_KIND = "human_subject_filter"
CLASSIFICATION_VERSION = "human-subject-title-abstract-v1"


parser = argparse.ArgumentParser()
parser.add_argument("--limit", type=int, help="Stop after processing this many articles")
parser.add_argument("--journal", action="append", help="Filter by journal name; can be specified multiple times")
parser.add_argument("--pub-year", type=int, help="Only submit articles from this publication year")
parser.add_argument("--output-file", help="Where to put the batch file (default: random tempfile)")
parser.add_argument("--dry-run", action="store_true", help="Don't send the batch to OpenAI")
parser.add_argument("--batch-id-save-file", help="What file to put the local batch ID into")
parser.add_argument("--model", default="gpt-5.4-mini", help="OpenAI model to use for this classifier")
parser.add_argument(
    "--openai-api-key",
    default=os.environ.get("OPENAI_API_KEY_FILE", os.path.expanduser("~/.openai.lig.key")),
)
parser.add_argument("--explain-queries", action="store_true", help="Run EXPLAIN on all queries and log to file")
parser.add_argument("--explain-log", default="human_subject_bulkquery_explains.log", help="Log file for EXPLAIN output")
args = parser.parse_args()

if args.output_file is None:
    tf = tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".jsonl")
    args.output_file = tf.name
    tf.close()

conn = psycopg2.connect("")
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cursor.execute("SET search_path TO languageingenetics, public")


def execute_query(cursor_to_use, sql, params=None):
    """Execute a query, optionally logging EXPLAIN output."""
    if args.explain_queries:
        explain_cursor = conn.cursor()
        try:
            if params:
                explain_cursor.execute("EXPLAIN (ANALYZE, BUFFERS, VERBOSE) " + sql, params)
            else:
                explain_cursor.execute("EXPLAIN (ANALYZE, BUFFERS, VERBOSE) " + sql)
            explain_output = "\n".join(row[0] for row in explain_cursor.fetchall())

            with open(args.explain_log, "a") as f:
                f.write(f"\n{'=' * 80}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Query:\n{sql}\n")
                if params:
                    f.write(f"Parameters: {params}\n")
                f.write(f"\nEXPLAIN output:\n{explain_output}\n")
        finally:
            explain_cursor.close()

    if params:
        cursor_to_use.execute(sql, params)
    else:
        cursor_to_use.execute(sql)
    return cursor_to_use


def ensure_schema():
    cursor.execute("""
        ALTER TABLE languageingenetics.batches
        ADD COLUMN IF NOT EXISTS batch_kind TEXT NOT NULL DEFAULT 'term_analysis'
    """)
    cursor.execute("""
        UPDATE languageingenetics.batches
        SET batch_kind = 'term_analysis'
        WHERE batch_kind IS NULL
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS languageingenetics.human_subject_classifications (
            id BIGSERIAL PRIMARY KEY,
            article_id BIGINT,
            work_id BIGINT,
            work_version_id BIGINT,
            has_abstract BOOLEAN,
            pub_year INTEGER,
            processed BOOLEAN NOT NULL DEFAULT false,
            batch_id INTEGER REFERENCES languageingenetics.batches(id),
            when_processed TIMESTAMPTZ,
            about_humans BOOLEAN,
            human_evidence TEXT,
            confidence TEXT,
            model TEXT,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            classification_version TEXT NOT NULL DEFAULT 'human-subject-title-abstract-v1',
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for column, definition in {
        "article_id": "BIGINT",
        "work_id": "BIGINT",
        "work_version_id": "BIGINT",
        "has_abstract": "BOOLEAN",
        "pub_year": "INTEGER",
        "processed": "BOOLEAN NOT NULL DEFAULT false",
        "batch_id": "INTEGER REFERENCES languageingenetics.batches(id)",
        "when_processed": "TIMESTAMPTZ",
        "about_humans": "BOOLEAN",
        "human_evidence": "TEXT",
        "confidence": "TEXT",
        "model": "TEXT",
        "prompt_tokens": "INTEGER",
        "completion_tokens": "INTEGER",
        "classification_version": f"TEXT NOT NULL DEFAULT '{CLASSIFICATION_VERSION}'",
        "created_at": "TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP",
    }.items():
        cursor.execute(f"""
            ALTER TABLE languageingenetics.human_subject_classifications
            ADD COLUMN IF NOT EXISTS {column} {definition}
        """)
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS human_subject_classifications_work_version_uidx
            ON languageingenetics.human_subject_classifications (work_version_id)
            WHERE work_version_id IS NOT NULL
    """)
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS human_subject_classifications_article_uidx
            ON languageingenetics.human_subject_classifications (article_id)
            WHERE article_id IS NOT NULL
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS human_subject_classifications_batch_idx
            ON languageingenetics.human_subject_classifications (batch_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS human_subject_classifications_processed_work_version_idx
            ON languageingenetics.human_subject_classifications (processed, work_version_id)
            WHERE work_version_id IS NOT NULL
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS languageingenetics.batch_diagnostics (
            id BIGSERIAL PRIMARY KEY,
            batch_id INT NOT NULL REFERENCES languageingenetics.batches(id) ON DELETE CASCADE,
            article_id BIGINT,
            event_type TEXT NOT NULL,
            details JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("ALTER TABLE languageingenetics.batch_diagnostics ALTER COLUMN article_id TYPE BIGINT")
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS batch_diagnostics_batch_id_idx
            ON languageingenetics.batch_diagnostics (batch_id)
    """)
    conn.commit()


ensure_schema()

if args.explain_queries:
    with open(args.explain_log, "w") as f:
        f.write(f"Query Explanation Log - Generated {datetime.now().isoformat()}\n")
        f.write(f"{'=' * 80}\n")

stats = Counter()

cursor.execute("BEGIN")
cursor.execute(
    "INSERT INTO languageingenetics.batches (batch_kind) VALUES (%s) RETURNING id",
    [BATCH_KIND],
)
batch_id = cursor.fetchone()["id"]


def record_diagnostic(event_type, article_id, **details):
    cursor.execute(
        """
        INSERT INTO languageingenetics.batch_diagnostics (batch_id, article_id, event_type, details)
        VALUES (%s, %s, %s, %s)
        """,
        [batch_id, article_id, event_type, psycopg2.extras.Json(details) if details else None],
    )


tools = [{
    "type": "function",
    "function": {
        "name": "classify_human_subject",
        "description": "Classify whether article title and abstract are about humans (Homo sapiens).",
        "parameters": {
            "type": "object",
            "properties": {
                "about_humans": {
                    "type": "boolean",
                    "description": "True when the title or abstract indicates the paper is about humans, Homo sapiens."
                },
                "evidence": {
                    "type": "string",
                    "description": "A short phrase naming the title/abstract evidence for the decision, or blank if none."
                },
                "confidence": {
                    "type": "string",
                    "enum": ["high", "medium", "low"],
                    "description": "Confidence in the classification."
                },
            },
            "required": ["about_humans", "evidence", "confidence"],
        },
    },
}]


def process_article(row):
    stats["examined"] += 1

    article_id = row.get("article_id")
    work_id = row.get("work_id")
    work_version_id = row.get("work_version_id")
    title = row.get("title")
    abstract = row.get("abstract")
    pub_year = row.get("pub_year")
    diagnostic_id = article_id if article_id is not None else work_version_id

    if work_version_id is None and article_id is None:
        stats["missing_metadata"] += 1
        record_diagnostic("human_subject_skipped", diagnostic_id, reason="missing_identifier")
        return False

    execute_query(
        cursor,
        """
        SELECT id
        FROM languageingenetics.human_subject_classifications
        WHERE (work_version_id = %s AND %s IS NOT NULL)
           OR (article_id = %s AND %s IS NOT NULL)
        """,
        [work_version_id, work_version_id, article_id, article_id],
    )
    if cursor.fetchone() is not None:
        stats["already_classified"] += 1
        record_diagnostic("human_subject_skipped", diagnostic_id, reason="already_classified")
        return False

    if not title:
        stats["missing_title"] += 1
        record_diagnostic("human_subject_skipped", diagnostic_id, reason="missing_title")
        print(f"Warning: No title found for work version {diagnostic_id}", file=sys.stderr)
        return False

    prompt = (
        "Based only on the title and abstract, decide whether this genetics article is about "
        "humans, Homo sapiens.\n\n"
        "Return about_humans=true when the article studies, reviews, discusses, or analyzes "
        "humans, human populations, patients, families, human diseases, human genetics/genomics, "
        "or human-derived samples/data. Return about_humans=false when it is about a non-human "
        "species, a generic method with no stated human application, or the title/abstract does "
        "not make a human focus clear. Do not infer from the journal name.\n\n"
        f"TITLE: {title}\n"
    )
    if abstract:
        prompt += f"ABSTRACT: {abstract}\n"

    cursor.execute(
        """
        INSERT INTO languageingenetics.human_subject_classifications (
            article_id,
            work_id,
            work_version_id,
            has_abstract,
            pub_year,
            batch_id,
            classification_version
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        [article_id, work_id, work_version_id, abstract is not None, pub_year, batch_id, CLASSIFICATION_VERSION],
    )
    classification_id = cursor.fetchone()["id"]

    batch_text = {
        "custom_id": f"human:{classification_id}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": args.model,
            "messages": [{"role": "user", "content": prompt}],
            "tools": tools,
            "tool_choice": {"type": "function", "function": {"name": "classify_human_subject"}},
        },
    }

    with open(args.output_file, "a") as f:
        f.write(json.dumps(batch_text) + "\n")

    stats["submitted"] += 1
    record_diagnostic(
        "human_subject_submitted",
        diagnostic_id,
        classification_id=classification_id,
        work_id=work_id,
        work_version_id=work_version_id,
        has_abstract=abstract is not None,
        pub_year=pub_year,
    )
    return True


journals_to_query = []
if args.journal:
    journals_to_query = args.journal
else:
    execute_query(cursor, "SELECT name FROM languageingenetics.journals WHERE enabled = true")
    journals_to_query = [row["name"] for row in cursor]
    if journals_to_query:
        print(f"Processing {len(journals_to_query)} enabled journals from database", file=sys.stderr)

query_params = []
limit_sql = f"\n        LIMIT {args.limit}" if args.limit else ""
year_filter_sql = "\n          AND cw.pub_year = %s" if args.pub_year is not None else ""

if journals_to_query:
    subqueries = []
    for journal in journals_to_query:
        subqueries.append(f"""
        (
        SELECT
            NULL::INT AS article_id,
            cw.work_id,
            cw.work_version_id,
            cw.title,
            cw.abstract,
            cw.pub_year
        FROM public.crossref_current_works cw
        WHERE cw.journal_name = %s
          AND cw.title IS NOT NULL
          {year_filter_sql}
          AND NOT EXISTS (
              SELECT 1
              FROM languageingenetics.human_subject_classifications h
              WHERE h.work_version_id = cw.work_version_id
          )
        ORDER BY cw.pub_year DESC NULLS LAST, cw.work_version_id
        {limit_sql}
        )
        """)
        query_params.append(journal)
        if args.pub_year is not None:
            query_params.append(args.pub_year)

    query = "SELECT * FROM (\n" + "\n    UNION ALL\n".join(subqueries) + "\n) AS combined"

    if args.limit:
        query += f" ORDER BY pub_year DESC NULLS LAST, work_version_id LIMIT {args.limit}"
else:
    if args.pub_year is not None:
        query_params.append(args.pub_year)
    query = f"""
    SELECT
        NULL::INT AS article_id,
        cw.work_id,
        cw.work_version_id,
        cw.title,
        cw.abstract,
        cw.pub_year
    FROM public.crossref_current_works cw
    WHERE cw.title IS NOT NULL
      {year_filter_sql}
      AND NOT EXISTS (
          SELECT 1
          FROM languageingenetics.human_subject_classifications h
          WHERE h.work_version_id = cw.work_version_id
      )
    ORDER BY cw.pub_year DESC NULLS LAST, cw.work_version_id
    {limit_sql}
    """

article_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
execute_query(article_cursor, query, query_params)

processed_count = 0
for row in article_cursor:
    if process_article(row):
        processed_count += 1
        if processed_count % 100 == 0:
            print(f"Processed {processed_count} articles...", file=sys.stderr)

article_cursor.close()

if processed_count == 0:
    print("No human-subject classifications were queued", file=sys.stderr)
    conn.rollback()
    conn.close()
    sys.exit(1)

print("Human-subject batch preparation diagnostics:", file=sys.stderr)
print(f"  Articles examined: {stats['examined']}", file=sys.stderr)
print(f"  Submitted to batch: {stats['submitted']}", file=sys.stderr)
print(f"  Skipped (already classified): {stats['already_classified']}", file=sys.stderr)
print(f"  Skipped (missing title): {stats['missing_title']}", file=sys.stderr)
if stats["missing_metadata"]:
    print(f"  Skipped (missing metadata): {stats['missing_metadata']}", file=sys.stderr)

record_diagnostic(
    "human_subject_summary",
    None,
    totals={key: int(value) for key, value in stats.items()},
)

if args.dry_run:
    conn.rollback()
    conn.close()
    sys.exit(0)

api_key = open(args.openai_api_key).read().strip()
client = openai.OpenAI(api_key=api_key)

batch_input_file = client.files.create(
    file=open(args.output_file, "rb"),
    purpose="batch",
)

result = client.batches.create(
    input_file_id=batch_input_file.id,
    endpoint="/v1/chat/completions",
    completion_window="24h",
    metadata={
        "description": f"batch {batch_id} (human-subject filter)",
        "local_batch_id": f"{batch_id}",
        "batch_kind": BATCH_KIND,
        "model": args.model,
    },
)

cursor.execute(
    """
    UPDATE languageingenetics.batches
    SET openai_batch_id = %s,
        when_sent = CURRENT_TIMESTAMP,
        batch_kind = %s
    WHERE id = %s
    """,
    [result.id, BATCH_KIND, batch_id],
)

if cursor.rowcount != 1:
    conn.rollback()
    conn.close()
    sys.exit(f"Unexpectedly updated {cursor.rowcount} rows when setting OpenAI batch ID {result.id}")

conn.commit()
conn.close()

if args.explain_queries:
    print(f"Query explanations written to {args.explain_log}", file=sys.stderr)

if args.batch_id_save_file:
    with open(args.batch_id_save_file, "w") as bisf:
        bisf.write(f"{batch_id}")
