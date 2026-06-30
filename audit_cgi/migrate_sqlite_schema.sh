#!/bin/sh
# Idempotent SQLite migrations for the live LIG audit database.

set -eu

DB="${1:-../db/lig_audit.db}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"

sqlite3 "$DB" < "$SCRIPT_DIR/init_schema.sql"

has_column() {
    table="$1"
    column="$2"
    sqlite3 "$DB" "PRAGMA table_info($table);" | awk -F'|' '{print $2}' | grep -qx "$column"
}

add_column() {
    table="$1"
    column="$2"
    definition="$3"
    if ! has_column "$table" "$column"; then
        sqlite3 "$DB" "ALTER TABLE $table ADD COLUMN $column $definition;"
    fi
}

drop_column() {
    table="$1"
    column="$2"
    if has_column "$table" "$column"; then
        sqlite3 "$DB" "ALTER TABLE $table DROP COLUMN $column;"
    fi
}

add_column fulltext_articles ai_analysis_status "TEXT NOT NULL DEFAULT 'not_queued'"
add_column fulltext_articles ai_caucasian "INTEGER CHECK (ai_caucasian IN (0, 1))"
add_column fulltext_articles ai_white "INTEGER CHECK (ai_white IN (0, 1))"
add_column fulltext_articles ai_european "INTEGER CHECK (ai_european IN (0, 1))"
add_column fulltext_articles ai_european_phrase_used "TEXT"
add_column fulltext_articles ai_other "INTEGER CHECK (ai_other IN (0, 1))"
add_column fulltext_articles ai_other_phrase_used "TEXT"
add_column fulltext_articles ai_model "TEXT"
add_column fulltext_articles ai_prompt_tokens "INTEGER"
add_column fulltext_articles ai_completion_tokens "INTEGER"
add_column fulltext_articles ai_error "TEXT"
add_column fulltext_articles ai_processed_at "TEXT"
add_column fulltext_articles uploaded_filename "TEXT"
add_column fulltext_articles uploaded_content_type "TEXT"
add_column fulltext_articles uploaded_size "INTEGER"
add_column fulltext_articles uploaded_file_path "TEXT"
add_column fulltext_articles uploaded_blob "BLOB"
add_column fulltext_articles uploaded_at "TEXT"
if has_column fulltext_articles fulltext_path; then
    sqlite3 "$DB" "UPDATE fulltext_articles SET uploaded_file_path = COALESCE(NULLIF(uploaded_file_path, ''), fulltext_path) WHERE fulltext_path IS NOT NULL AND fulltext_path <> '';"
    drop_column fulltext_articles fulltext_path
fi

sqlite3 "$DB" "CREATE INDEX IF NOT EXISTS fulltext_articles_ai_status_idx ON fulltext_articles (ai_analysis_status, batch_slug, article_id);"
