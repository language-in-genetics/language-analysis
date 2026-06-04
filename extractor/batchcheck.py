#!/usr/bin/env python3

import argparse
import os
import sys
import openai
import psycopg2
import psycopg2.extras
import time

parser = argparse.ArgumentParser()
parser.add_argument(
    "--openai-api-key",
    default=os.environ.get("OPENAI_API_KEY_FILE", os.path.expanduser("~/.openai.lig.key")),
)
parser.add_argument("--only-batch", type=int, help="The batch ID to look at")
parser.add_argument("--monitor", action="store_true", help="Monitor in a loop until the status is 'completed'. Only makes sense with --only-batch")
parser.add_argument("--quiet", action="store_true", help="Suppress non-error output")
args = parser.parse_args()

if args.quiet and args.monitor:
    parser.error("--quiet cannot be used together with --monitor")

api_key = open(args.openai_api_key).read().strip()
client = openai.OpenAI(api_key=api_key)

# Connect using environment variables (PGDATABASE, PGHOST, etc.)
conn = psycopg2.connect("")
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Set search path
cursor.execute("SET search_path TO languageingenetics, public")
cursor.execute("""
    ALTER TABLE languageingenetics.batches
    ADD COLUMN IF NOT EXISTS batch_kind TEXT NOT NULL DEFAULT 'term_analysis'
""")
cursor.execute("""
    UPDATE languageingenetics.batches
    SET batch_kind = 'term_analysis'
    WHERE batch_kind IS NULL
""")

# Create progress tracking table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS languageingenetics.batchprogress (
        batch_id INT REFERENCES languageingenetics.batches(id),
        when_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        number_completed INT,
        number_failed INT
    )
""")
conn.commit()

cursor.execute("SELECT to_regclass('languageingenetics.human_subject_classifications') AS table_name")
has_human_subject_table = cursor.fetchone()["table_name"] is not None

if has_human_subject_table:
    query = """
        SELECT
            b.id,
            b.openai_batch_id,
            COALESCE(b.batch_kind, 'term_analysis') AS batch_kind,
            CASE
                WHEN b.batch_kind = 'human_subject_filter' THEN COALESCE(h.file_count, 0)
                ELSE COALESCE(f.file_count, 0)
            END AS file_count
        FROM languageingenetics.batches b
        LEFT JOIN (
            SELECT batch_id, COUNT(*) AS file_count
            FROM languageingenetics.files
            GROUP BY batch_id
        ) f ON f.batch_id = b.id
        LEFT JOIN (
            SELECT batch_id, COUNT(*) AS file_count
            FROM languageingenetics.human_subject_classifications
            GROUP BY batch_id
        ) h ON h.batch_id = b.id
        WHERE b.when_sent IS NOT NULL
          AND b.when_retrieved IS NULL
    """
else:
    query = """
        SELECT
            b.id,
            b.openai_batch_id,
            COALESCE(b.batch_kind, 'term_analysis') AS batch_kind,
            COUNT(f.id) AS file_count
        FROM languageingenetics.batches b
        JOIN languageingenetics.files f ON f.batch_id = b.id
        WHERE b.when_sent IS NOT NULL
          AND b.when_retrieved IS NULL
    """

if args.only_batch:
    query += f"AND b.id = {int(args.only_batch)} "
if has_human_subject_table:
    query += "ORDER BY b.id"
else:
    query += "GROUP BY b.id, b.openai_batch_id, b.batch_kind ORDER BY b.id"

if args.monitor:
    import tqdm
    progress = None

while True:
    cursor.execute(query)
    work_to_be_done = False

    batches = cursor.fetchall()

    throughput_estimates = []

    for batch_row in batches:
        local_batch_id = batch_row['id']
        openai_batch_id = batch_row['openai_batch_id']
        number_of_files = batch_row['file_count'] or 0
        batch_kind = batch_row['batch_kind']

        openai_result = client.batches.retrieve(openai_batch_id)
        metadata = openai_result.metadata or {}
        if openai_result.status == 'completed':
            work_to_be_done = True

        if openai_result.status in ['in_progress', 'completed']:
            cursor.execute(
                "INSERT INTO languageingenetics.batchprogress (batch_id, number_completed, number_failed) VALUES (%s, %s, %s)",
                [local_batch_id, openai_result.request_counts.completed, openai_result.request_counts.failed]
            )
            conn.commit()

            cursor.execute(
                """
                SELECT when_checked, number_completed
                FROM languageingenetics.batchprogress
                WHERE batch_id = %s
                ORDER BY when_checked DESC
                LIMIT 2
                """,
                [local_batch_id]
            )
            history = cursor.fetchall()
            if len(history) == 2:
                latest, previous = history[0], history[1]
                delta_completed = latest['number_completed'] - previous['number_completed']
                delta_seconds = (latest['when_checked'] - previous['when_checked']).total_seconds()
                if delta_completed > 0 and delta_seconds > 0:
                    throughput_estimates.append(delta_completed * 3600.0 / delta_seconds)

        if args.monitor:
            if progress is None:
                progress = tqdm.tqdm(total=number_of_files)
            progress.set_description(openai_result.status)
            if openai_result.status in ['in_progress', 'completed']:
                progress.update(openai_result.request_counts.completed - progress.n)
            if openai_result.status == 'completed':
                break
            time.sleep(15)
            continue

        if not args.quiet:
            print(f"""## {metadata.get('description')}
      Num files: {number_of_files}
           Kind: {batch_kind}
       Local ID: {local_batch_id}
       Returned: {metadata.get('local_batch_id')}
       Batch ID: {openai_batch_id}
        Created: {time.asctime(time.localtime(openai_result.created_at))}
         Status: {openai_result.status}""")

        if openai_result.errors:
            print("      Errors: ", file=sys.stderr)
            for err in openai_result.errors.data:
                print(f"         - {err.code} on line {err.line}: {err.message}", file=sys.stderr)

        if openai_result.request_counts and not args.quiet:
            print(f"       Progress: {openai_result.request_counts.completed}/{openai_result.request_counts.total}")
            print(f"       Failures: {openai_result.request_counts.failed}")

        if not args.quiet:
            print()

    if throughput_estimates and not args.quiet:
        total_throughput = sum(throughput_estimates)
        print(f"Estimated throughput: {total_throughput:.1f} articles/hour across {len(throughput_estimates)} batch(es)")

    if not args.monitor:
        break
    if work_to_be_done:
        break

if work_to_be_done:
    sys.exit(0)
else:
    sys.exit(1)
