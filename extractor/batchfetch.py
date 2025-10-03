#!/usr/bin/env python3

import argparse
import os
import sys
import openai
import psycopg2
import psycopg2.extras
import time
import json

parser = argparse.ArgumentParser()
parser.add_argument("--openai-api-key", default=os.path.expanduser("~/.openai.key"))
parser.add_argument("--progress-bar", action='store_true', help="Show a progress bar for updating the database on each batch")
parser.add_argument("--report-costs", action="store_true", help="Report the cost of the runs fetched")
args = parser.parse_args()

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

batches = cursor.fetchall()

for batch_row in batches:
    local_batch_id = batch_row['id']
    openai_batch_id = batch_row['openai_batch_id']
    openai_result = client.batches.retrieve(openai_batch_id)
    if openai_result.status != 'completed':
        continue
    if openai_result.error_file_id is not None:
        error_file_response = client.files.content(openai_result.error_file_id)
        sys.stderr.write(error_file_response.text)
    if openai_result.output_file_id is None:
        continue
    
    file_response = client.files.content(openai_result.output_file_id)
    iterator = file_response.text.splitlines()
    
    if args.progress_bar:
        import tqdm
        iterator = tqdm.tqdm(iterator)
        if 'description' in openai_result.metadata:
            iterator.set_description(openai_result.metadata['description'])
    
    for row in iterator:
        record = json.loads(row)
        if record['response']['status_code'] != 200:
            continue
        
        # Extract the tool call arguments
        arguments = json.loads(record['response']['body']['choices'][0]['message']['tool_calls'][0]['function']['arguments'])

        # Get the article_id (which was used as the custom_id)
        article_id = int(record['custom_id'])

        # Extract usage information
        usage = record['response']['body']['usage']
        model = record['response']['body']['model'] + " (batch)"

        # Update the files table with the analysis results
        cursor.execute("""
            UPDATE languageingenetics.files SET
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
            WHERE article_id = %s
        """, [
            arguments['caucasian'],
            arguments['white'],
            arguments['european'],
            arguments['european_phrase_used'],
            arguments['other'],
            arguments['other_phrase_used'],
            usage['prompt_tokens'],
            usage['completion_tokens'],
            article_id
        ])

        total_prompt_tokens += usage['prompt_tokens']
        total_completion_tokens += usage['completion_tokens']

    # Mark the batch as retrieved
    cursor.execute("UPDATE languageingenetics.batches SET when_retrieved = CURRENT_TIMESTAMP WHERE id = %s", [local_batch_id])
    conn.commit()

conn.close()

if args.report_costs:
    print(f"Prompt tokens:     {total_prompt_tokens}")
    print(f"Completion tokens: {total_completion_tokens}")
    prompt_pricing = 0.075 / 1000000  # Adjust pricing as needed
    completion_pricing = 0.3 / 1000000  # Adjust pricing as needed
    cost = prompt_pricing * total_prompt_tokens + completion_pricing * total_completion_tokens
    print(f"Cost (USD):        {cost:.2f}")
