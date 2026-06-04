#!/usr/bin/env python3

import argparse
import json
import logging
import os
import sys

import openai
import psycopg2
import psycopg2.extras


BATCH_KIND = "human_subject_filter"
CLASSIFICATION_VERSION = "human-subject-title-abstract-v1"


parser = argparse.ArgumentParser()
parser.add_argument(
    "--openai-api-key",
    default=os.environ.get("OPENAI_API_KEY_FILE", os.path.expanduser("~/.openai.lig.key")),
)
parser.add_argument("--progress-bar", action="store_true", help="Show a progress bar for updating the database on each batch")
parser.add_argument("--report-costs", action="store_true", help="Report token usage for the runs fetched")
parser.add_argument("--error-log", default="human_subject_batchfetch_errors.log", help="Path to error log file")
args = parser.parse_args()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(args.error_log),
        logging.StreamHandler(sys.stderr),
    ],
)


def sanitize_string(s):
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    sanitized = s.replace("\x00", "")
    if sanitized != s:
        logging.warning(f"Removed {s.count(chr(0))} NUL character(s) from string")
    return sanitized


def custom_id_target(custom_id):
    custom_id = str(custom_id)
    if not custom_id.startswith("human:"):
        raise ValueError(f"Unexpected custom_id for human-subject batch: {custom_id}")
    return int(custom_id.split(":", 1)[1])


def ensure_schema(cursor, conn):
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
    conn.commit()


api_key = open(args.openai_api_key).read().strip()
client = openai.OpenAI(api_key=api_key)

conn = psycopg2.connect("")
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cursor.execute("SET search_path TO languageingenetics, public")
ensure_schema(cursor, conn)

cursor.execute("""
    SELECT id, openai_batch_id
    FROM languageingenetics.batches
    WHERE when_sent IS NOT NULL
      AND when_retrieved IS NULL
      AND batch_kind = %s
""", [BATCH_KIND])

total_prompt_tokens = 0
total_completion_tokens = 0
total_records_processed = 0
total_records_failed = 0
total_batches_processed = 0

batches = cursor.fetchall()
logging.info(f"Found {len(batches)} human-subject batch(es) to process")

for batch_row in batches:
    local_batch_id = batch_row["id"]
    openai_batch_id = batch_row["openai_batch_id"]
    batch_records_processed = 0
    batch_records_failed = 0

    logging.info(f"Processing human-subject batch {local_batch_id} (OpenAI ID: {openai_batch_id})")

    openai_result = client.batches.retrieve(openai_batch_id)
    if openai_result.status != "completed":
        logging.info(f"Batch {local_batch_id} status: {openai_result.status} (skipping)")
        continue
    if openai_result.error_file_id is not None:
        error_file_response = client.files.content(openai_result.error_file_id)
        logging.error(f"Batch {local_batch_id} has errors:\n{error_file_response.text}")
        sys.stderr.write(error_file_response.text)
    if openai_result.output_file_id is None:
        logging.warning(f"Batch {local_batch_id} has no output file (skipping)")
        continue

    file_response = client.files.content(openai_result.output_file_id)
    iterator = file_response.text.splitlines()

    if args.progress_bar:
        import tqdm
        iterator = tqdm.tqdm(iterator)
        metadata = openai_result.metadata or {}
        if "description" in metadata:
            iterator.set_description(metadata["description"])

    for row in iterator:
        try:
            record = json.loads(row)
            if record["response"]["status_code"] != 200:
                continue

            arguments = json.loads(
                record["response"]["body"]["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]
            )

            classification_id = custom_id_target(record["custom_id"])
            usage = record["response"]["body"]["usage"]
            model = record["response"]["body"]["model"] + " (batch)"
            about_humans = arguments.get("about_humans", False)
            if not isinstance(about_humans, bool):
                raise ValueError(f"about_humans was not boolean: {about_humans!r}")

            evidence = sanitize_string(arguments.get("evidence", ""))
            confidence = sanitize_string(arguments.get("confidence", ""))

            cursor.execute("""
                UPDATE languageingenetics.human_subject_classifications
                SET
                    processed = true,
                    when_processed = CURRENT_TIMESTAMP,
                    about_humans = %s,
                    human_evidence = %s,
                    confidence = %s,
                    model = %s,
                    prompt_tokens = %s,
                    completion_tokens = %s
                WHERE id = %s
            """, [
                about_humans,
                evidence,
                confidence,
                model,
                usage["prompt_tokens"],
                usage["completion_tokens"],
                classification_id,
            ])
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"updated {cursor.rowcount} human_subject_classifications rows for custom_id {record['custom_id']}"
                )

            total_prompt_tokens += usage["prompt_tokens"]
            total_completion_tokens += usage["completion_tokens"]
            batch_records_processed += 1
            total_records_processed += 1

        except Exception as e:
            batch_records_failed += 1
            total_records_failed += 1

            logging.error(f"Failed to process record in human-subject batch {local_batch_id}")
            logging.error(f"Error type: {type(e).__name__}")
            logging.error(f"Error message: {str(e)}")
            try:
                logging.error(f"Classification ID: {custom_id_target(record.get('custom_id', 'unknown'))}")
            except Exception:
                logging.error("Could not extract classification ID from custom_id")
            try:
                if "arguments" in locals():
                    logging.error(f"Arguments: {json.dumps(arguments, indent=2)}")
            except Exception as log_error:
                logging.error(f"Could not log arguments: {log_error}")
            try:
                logging.error(f"Raw record: {row[:500]}...")
            except Exception:
                pass
            logging.error("---")
            continue

    cursor.execute(
        "UPDATE languageingenetics.batches SET when_retrieved = CURRENT_TIMESTAMP WHERE id = %s",
        [local_batch_id],
    )
    conn.commit()
    total_batches_processed += 1
    logging.info(f"Batch {local_batch_id} complete: {batch_records_processed} succeeded, {batch_records_failed} failed")

conn.close()

logging.info("=" * 60)
logging.info("Human-subject processing complete")
logging.info(f"Batches processed: {total_batches_processed}")
logging.info(f"Records succeeded: {total_records_processed}")
logging.info(f"Records failed: {total_records_failed}")
if total_records_failed > 0:
    logging.warning(f"Failed records logged to: {args.error_log}")
logging.info("=" * 60)

if args.report_costs:
    print(f"Prompt tokens:     {total_prompt_tokens}")
    print(f"Completion tokens: {total_completion_tokens}")

if total_records_failed > 0 and total_records_processed > 0:
    logging.warning("Exiting with code 0 (partial success)")
    sys.exit(0)
elif total_records_failed > 0:
    logging.error("Exiting with code 1 (all records failed)")
    sys.exit(1)
else:
    sys.exit(0)
