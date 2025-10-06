#!/usr/bin/env python3

import argparse
import psycopg2
import psycopg2.extras

parser = argparse.ArgumentParser(description="Delete a failed batch from the database so articles can be retried")
parser.add_argument("batch_id", type=int, help="Local batch ID to delete")
parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without actually deleting")
args = parser.parse_args()

# Connect using environment variables (PGDATABASE, PGHOST, etc.)
conn = psycopg2.connect("")
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Set search path
cursor.execute("SET search_path TO languageingenetics, public")

# Get batch info
cursor.execute("SELECT id, openai_batch_id, when_created, when_sent, when_retrieved FROM languageingenetics.batches WHERE id = %s", [args.batch_id])
batch = cursor.fetchone()

if not batch:
    print(f"Batch {args.batch_id} not found")
    conn.close()
    exit(1)

print(f"Batch {batch['id']}:")
print(f"  OpenAI Batch ID: {batch['openai_batch_id']}")
print(f"  Created: {batch['when_created']}")
print(f"  Sent: {batch['when_sent']}")
print(f"  Retrieved: {batch['when_retrieved']}")

# Count files associated with this batch
cursor.execute("SELECT COUNT(*) as count FROM languageingenetics.files WHERE batch_id = %s", [args.batch_id])
file_count = cursor.fetchone()['count']
print(f"  Files: {file_count}")

if args.dry_run:
    print("\nDry run - no changes made")
    conn.close()
    exit(0)

# Delete files, progress records, and batch
print(f"\nDeleting {file_count} files and batch {args.batch_id}...")
cursor.execute("DELETE FROM languageingenetics.files WHERE batch_id = %s", [args.batch_id])
print(f"Deleted {cursor.rowcount} files")

cursor.execute("DELETE FROM languageingenetics.batchprogress WHERE batch_id = %s", [args.batch_id])
if cursor.rowcount > 0:
    print(f"Deleted {cursor.rowcount} progress records")

cursor.execute("DELETE FROM languageingenetics.batches WHERE id = %s", [args.batch_id])
print(f"Deleted batch {args.batch_id}")

conn.commit()
conn.close()

print("Done - articles can now be reprocessed")
