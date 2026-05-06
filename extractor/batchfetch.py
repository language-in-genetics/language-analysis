#!/usr/bin/env python3

import argparse
import os
import sys
import openai
import psycopg2
import psycopg2.extras
import time
import json
import logging
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument(
    "--openai-api-key",
    default=os.environ.get("OPENAI_API_KEY_FILE", os.path.expanduser("~/.openai.lig.key")),
)
parser.add_argument("--progress-bar", action='store_true', help="Show a progress bar for updating the database on each batch")
parser.add_argument("--report-costs", action="store_true", help="Report the cost of the runs fetched")
parser.add_argument("--error-log", default="batchfetch_errors.log", help="Path to error log file")
args = parser.parse_args()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(args.error_log),
        logging.StreamHandler(sys.stderr)
    ]
)

def sanitize_string(s):
    """Remove NUL characters and other problematic characters from strings."""
    if s is None:
        return ''
    if not isinstance(s, str):
        s = str(s)
    # Remove NUL characters (0x00)
    sanitized = s.replace('\x00', '')
    if sanitized != s:
        logging.warning(f"Removed {s.count(chr(0))} NUL character(s) from string")
    return sanitized


def custom_id_target(custom_id):
    """Return the files table lookup column and value for an OpenAI custom_id."""
    custom_id = str(custom_id)
    if custom_id.startswith("file:"):
        return "id", int(custom_id.split(":", 1)[1])
    return "article_id", int(custom_id)

api_key = open(args.openai_api_key).read().strip()
client = openai.OpenAI(api_key=api_key)

# Connect using environment variables (PGDATABASE, PGHOST, etc.)
conn = psycopg2.connect("")
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Set search path
cursor.execute("SET search_path TO languageingenetics, public")

cursor.execute("SELECT id, openai_batch_id FROM languageingenetics.batches WHERE when_sent IS NOT NULL AND when_retrieved IS NULL")

total_prompt_tokens = 0
total_completion_tokens = 0
total_records_processed = 0
total_records_failed = 0
total_batches_processed = 0

batches = cursor.fetchall()

logging.info(f"Found {len(batches)} batch(es) to process")

for batch_row in batches:
    local_batch_id = batch_row['id']
    openai_batch_id = batch_row['openai_batch_id']
    batch_records_processed = 0
    batch_records_failed = 0

    logging.info(f"Processing batch {local_batch_id} (OpenAI ID: {openai_batch_id})")

    openai_result = client.batches.retrieve(openai_batch_id)
    if openai_result.status != 'completed':
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
        if 'description' in openai_result.metadata:
            iterator.set_description(openai_result.metadata['description'])
    
    for row in iterator:
        try:
            record = json.loads(row)
            if record['response']['status_code'] != 200:
                continue

            # Extract the tool call arguments
            arguments = json.loads(record['response']['body']['choices'][0]['message']['tool_calls'][0]['function']['arguments'])

            lookup_column, lookup_value = custom_id_target(record['custom_id'])

            # Extract usage information
            usage = record['response']['body']['usage']
            model = record['response']['body']['model'] + " (batch)"

            # Sanitize string fields to remove NUL characters
            european_phrase = sanitize_string(arguments.get('european_phrase_used', ''))
            other_phrase = sanitize_string(arguments.get('other_phrase_used', ''))

            cursor.execute(f"""
                UPDATE languageingenetics.files
                SET
                    processed = true,
                    when_processed = CURRENT_TIMESTAMP,
                    caucasian = %s,
                    white = %s,
                    european = %s,
                    european_phrase_used = %s,
                    other = %s,
                    other_phrase_used = %s,
                    prompt_tokens = %s,
                    completion_tokens = %s
                WHERE {lookup_column} = %s
            """, [
                arguments.get('caucasian', False),
                arguments.get('white', False),
                arguments.get('european', False),
                european_phrase,
                arguments.get('other', False),
                other_phrase,
                usage['prompt_tokens'],
                usage['completion_tokens'],
                lookup_value
            ])
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"updated {cursor.rowcount} files rows for custom_id {record['custom_id']}"
                )

            total_prompt_tokens += usage['prompt_tokens']
            total_completion_tokens += usage['completion_tokens']
            batch_records_processed += 1
            total_records_processed += 1

        except Exception as e:
            batch_records_failed += 1
            total_records_failed += 1

            # Log detailed error information
            logging.error(f"Failed to process record in batch {local_batch_id}")
            logging.error(f"Error type: {type(e).__name__}")
            logging.error(f"Error message: {str(e)}")

            try:
                lookup_column, lookup_value = custom_id_target(record.get('custom_id', 'unknown'))
                logging.error(f"Lookup: {lookup_column}={lookup_value}")
            except:
                logging.error(f"Could not extract files lookup from custom_id")

            # Log the problematic data for debugging
            try:
                if 'arguments' in locals():
                    logging.error(f"Arguments: {json.dumps(arguments, indent=2)}")
                    # Log hex representation of strings to see NUL characters
                    for key in ['european_phrase_used', 'other_phrase_used']:
                        if key in arguments and arguments[key]:
                            value = arguments[key]
                            hex_repr = ' '.join(f'{ord(c):02x}' for c in value[:100])  # First 100 chars
                            logging.error(f"{key} (hex): {hex_repr}")
            except Exception as log_error:
                logging.error(f"Could not log arguments: {log_error}")

            # Log the raw record for complete debugging info
            try:
                logging.error(f"Raw record: {row[:500]}...")  # First 500 chars
            except:
                pass

            logging.error("---")

            # Continue processing other records
            continue

    # Mark the batch as retrieved
    cursor.execute("UPDATE languageingenetics.batches SET when_retrieved = CURRENT_TIMESTAMP WHERE id = %s", [local_batch_id])
    conn.commit()
    total_batches_processed += 1

    # Log batch summary
    logging.info(f"Batch {local_batch_id} complete: {batch_records_processed} succeeded, {batch_records_failed} failed")

conn.close()

# Log final summary
logging.info("=" * 60)
logging.info(f"Processing complete!")
logging.info(f"Batches processed: {total_batches_processed}")
logging.info(f"Records succeeded: {total_records_processed}")
logging.info(f"Records failed: {total_records_failed}")
if total_records_failed > 0:
    logging.warning(f"Failed records logged to: {args.error_log}")
logging.info("=" * 60)

if args.report_costs:
    print(f"Prompt tokens:     {total_prompt_tokens}")
    print(f"Completion tokens: {total_completion_tokens}")
    prompt_pricing = 0.075 / 1000000  # Adjust pricing as needed
    completion_pricing = 0.3 / 1000000  # Adjust pricing as needed
    cost = prompt_pricing * total_prompt_tokens + completion_pricing * total_completion_tokens
    print(f"Cost (USD):        {cost:.2f}")

# Exit with error code if there were failures (but still processed some records)
# This allows monitoring while still completing successfully
if total_records_failed > 0 and total_records_processed > 0:
    logging.warning("Exiting with code 0 (partial success)")
    sys.exit(0)
elif total_records_failed > 0:
    logging.error("Exiting with code 1 (all records failed)")
    sys.exit(1)
else:
    sys.exit(0)
