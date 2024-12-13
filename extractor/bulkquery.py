#!/usr/bin/env python3

import argparse
import json
import sqlite3
import time
import os
import openai
import sys
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument("directories", nargs="+", help="Directories containing metadata.json files")
parser.add_argument("--database", required=True, help="Where the database is")
parser.add_argument("--limit", type=int, help="Stop after processing this many files")
parser.add_argument("--output-file", help="Where to put the batch file (default: random tempfile)")
parser.add_argument("--dry-run", action="store_true", help="Don't send the batch to OpenAI")
parser.add_argument("--batch-id-save-file", help="What file to put the local batch ID into")
parser.add_argument("--openai-api-key", default=os.path.expanduser("~/.openai.key"))
args = parser.parse_args()

# Create output file if not specified
if args.output_file is None:
    tf = tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.jsonl')
    args.output_file = tf.name
    tf.close()

conn = sqlite3.connect(args.database)
cursor = conn.cursor()
update_cursor = conn.cursor()
cursor.execute("pragma busy_timeout = 30000;")
cursor.execute("pragma journal_mode = WAL;")
update_cursor.execute("pragma busy_timeout = 30000;")
update_cursor.execute("pragma journal_mode = WAL;")

# Create tables if they don't exist
update_cursor.execute("""
    create table if not exists files (
        id integer primary key autoincrement,
        filename text unique,
        has_abstract boolean,
        pub_year integer,
        processed boolean default false,
        batch_id integer references batches(id),
        when_processed datetime,
        caucasian boolean,
        white boolean,
        european boolean,
        european_phrase_used text,
        other boolean,
        other_phrase_used text
    )
""")

update_cursor.execute("""
    create table if not exists batches (
        id integer primary key autoincrement,
        openai_batch_id text,
        when_created datetime default current_timestamp,
        when_sent datetime,
        when_retrieved datetime
    )
""")

# Create indices
update_cursor.execute("create index if not exists files_by_filename on files(filename)")
update_cursor.execute("create index if not exists files_by_batch on files(batch_id) where processed = false")

# Start a transaction for this batch
update_cursor.execute("begin transaction")
update_cursor.execute("insert into batches default values")
batch_id = update_cursor.lastrowid

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

def process_file(filename):
    # Check if file is already processed
    cursor.execute("select id from files where filename = ?", [filename])
    if cursor.fetchone() is not None:
        return False

    try:
        with open(filename, 'r') as f:
            metadata = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: Could not parse JSON in {filename}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print(f"Error: File not found: {filename}", file=sys.stderr)
        return False

    # Extract information
    title = metadata.get('title', [None])[0]
    if not title:
        print(f"Warning: No title found in {filename}", file=sys.stderr)
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
        "custom_id": filename,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "tools": tools,
            "tool_choice": {"type": "function", "function": {"name": "analyze_text"}}
        }
    }

    # Write to batch file
    with open(args.output_file, 'a') as f:
        f.write(json.dumps(batch_text) + "\n")

    # Add to database
    update_cursor.execute(
        "insert into files (filename, has_abstract, pub_year, batch_id) values (?, ?, ?, ?)",
        [filename, abstract is not None, pub_year, batch_id]
    )

    return True

# Process files
processed_count = 0
for directory in args.directories:
    for dirpath, dirnames, filenames in os.walk(directory):
        if "metadata.json" in filenames:
            filename = os.path.join(dirpath, "metadata.json")
            if process_file(filename):
                processed_count += 1
                if args.limit and processed_count >= args.limit:
                    break
        if args.limit and processed_count >= args.limit:
            break

if processed_count == 0:
    print("No files were processed", file=sys.stderr)
    conn.rollback()
    sys.exit(1)

if args.dry_run:
    conn.rollback()
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
        "description": f"{args.database} batch {batch_id} (wordfreq)",
        "database": f"{args.database}",
        "local_batch_id": f"{batch_id}"
    }
)

update_cursor.execute(
    "update batches set openai_batch_id = ?, when_sent = current_timestamp where id = ?",
    [result.id, batch_id]
)

if update_cursor.rowcount != 1:
    sys.exit(f"Unexpectedly updated {update_cursor.rowcount} rows when we set the openai_batch id to {result.id} for batch {batch_id}")

conn.commit()

if args.batch_id_save_file:
    with open(args.batch_id_save_file, 'w') as bisf:
        bisf.write(f"{batch_id}")
