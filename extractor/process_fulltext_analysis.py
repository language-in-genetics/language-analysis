#!/usr/bin/env python3
"""
Process queued full-text uploads from the merah audit SQLite database.

This is intended for the raksasa cron path:
1. audit/sync_audit_db.sh pulls the live SQLite DB and upload files from merah.
2. import_fulltext_audit_reviews.py imports current upload and AI state to PostgreSQL.
3. this script extracts text where needed, runs the OpenAI analysis, and stores
   the result in both SQLite and PostgreSQL.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import openai
import psycopg2
import psycopg2.extras


TOOL_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "analyze_text",
            "description": "Analyze a full journal article for racial/ethnic terminology",
            "parameters": {
                "type": "object",
                "properties": {
                    "caucasian": {
                        "type": "boolean",
                        "description": "uses the word Caucasian, or similar",
                    },
                    "white": {
                        "type": "boolean",
                        "description": "uses the word white to refer to race, ancestry, ethnicity, population or equivalent",
                    },
                    "european": {
                        "type": "boolean",
                        "description": "uses a phrase like European ancestry",
                    },
                    "european_phrase_used": {
                        "type": "string",
                        "description": "the actual phrase used if european is true, blank otherwise",
                    },
                    "other": {
                        "type": "boolean",
                        "description": "uses some other phrase to describe someone with European/Caucasian/white ancestry, race, ethnicity or population",
                    },
                    "other_phrase_used": {
                        "type": "string",
                        "description": "what phrase was used if other is true, blank otherwise",
                    },
                },
                "required": [
                    "caucasian",
                    "white",
                    "european",
                    "european_phrase_used",
                    "other",
                    "other_phrase_used",
                ],
            },
        },
    }
]


class HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        stripped = data.strip()
        if stripped:
            self.parts.append(stripped)

    def text(self) -> str:
        return "\n".join(self.parts)


def get_pg_connection():
    return psycopg2.connect(
        dbname=os.environ.get("PGDATABASE", "crossref"),
        user=os.environ.get("PGUSER"),
        password=os.environ.get("PGPASSWORD"),
        host=os.environ.get("PGHOST"),
        port=os.environ.get("PGPORT", "5432"),
    )


def ensure_pg_schema(cur) -> None:
    schema_path = Path(__file__).resolve().parents[1] / "database" / "audit_schema.sql"
    cur.execute(schema_path.read_text())


def ensure_sqlite_ai_columns(conn: sqlite3.Connection) -> None:
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(fulltext_articles)")}
    additions = {
        "ai_analysis_status": "TEXT NOT NULL DEFAULT 'not_queued'",
        "ai_caucasian": "INTEGER CHECK (ai_caucasian IN (0, 1))",
        "ai_white": "INTEGER CHECK (ai_white IN (0, 1))",
        "ai_european": "INTEGER CHECK (ai_european IN (0, 1))",
        "ai_european_phrase_used": "TEXT",
        "ai_other": "INTEGER CHECK (ai_other IN (0, 1))",
        "ai_other_phrase_used": "TEXT",
        "ai_model": "TEXT",
        "ai_prompt_tokens": "INTEGER",
        "ai_completion_tokens": "INTEGER",
        "ai_error": "TEXT",
        "ai_processed_at": "TEXT",
    }
    for column, definition in additions.items():
        if column not in columns:
            conn.execute(f"ALTER TABLE fulltext_articles ADD COLUMN {column} {definition}")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS fulltext_articles_ai_status_idx
        ON fulltext_articles (ai_analysis_status, batch_slug, article_id)
        """
    )
    conn.commit()


def sanitize_string(value) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return value.replace("\x00", "")


def sqlite_bool(value: bool | None):
    if value is None:
        return None
    return 1 if value else 0


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def local_path_for_upload(sqlite_path: Path, fulltext_path: str | None) -> Path | None:
    if not fulltext_path:
        return None
    clean_path = str(fulltext_path).lstrip("/")
    if not clean_path.startswith("fulltext_uploads/"):
        return None
    return sqlite_path.parent / clean_path


def strip_html(raw: str) -> str:
    extractor = HTMLTextExtractor()
    extractor.feed(raw)
    return html.unescape(extractor.text())


def extract_file_text(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        if shutil.which("pdftotext") is None:
            raise RuntimeError("pdftotext is not installed on this host")
        result = subprocess.run(
            ["pdftotext", "-layout", str(path), "-"],
            check=True,
            text=True,
            capture_output=True,
            timeout=180,
        )
        return result.stdout
    raw = path.read_bytes()
    text = raw.decode("utf-8", errors="replace")
    if suffix in {".html", ".htm", ".xml"}:
        return strip_html(text)
    if suffix in {".txt", ".text", ".md", ".markdown"}:
        return text
    if "\x00" in text[:2048]:
        raise RuntimeError(f"unsupported binary upload type: {suffix or 'no extension'}")
    return text


def full_text_for_row(sqlite_path: Path, row: sqlite3.Row) -> str:
    existing = sanitize_string(row["extracted_text"]).strip()
    if existing:
        return existing
    local_path = local_path_for_upload(sqlite_path, row["fulltext_path"])
    if local_path is None:
        raise RuntimeError("queued article has no extracted text and no local upload path")
    if not local_path.exists():
        raise RuntimeError(f"uploaded file was not synced locally: {local_path}")
    return sanitize_string(extract_file_text(local_path)).strip()


def make_prompt(row: sqlite3.Row, full_text: str, max_chars: int) -> str:
    if len(full_text) > max_chars:
        full_text = full_text[:max_chars] + "\n\n[TRUNCATED FOR ANALYSIS]"
    prompt = (
        "Analyze the full journal article text below, not only the title and abstract. "
        "Does this article use terms like \"Caucasian\", \"white\", or \"European ancestry\" "
        "in a way that refers to race, ancestry, ethnicity, population, or an equivalent category?\n\n"
    )
    prompt += f"TITLE: {row['title'] or ''}\n"
    if row["abstract"]:
        prompt += f"ABSTRACT: {row['abstract']}\n"
    prompt += "\nFULL ARTICLE TEXT:\n"
    prompt += full_text
    return prompt


def analyze_article(client, model: str, row: sqlite3.Row, full_text: str, max_chars: int) -> tuple[dict, dict]:
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": make_prompt(row, full_text, max_chars)}],
        tools=TOOL_SCHEMA,
        tool_choice={"type": "function", "function": {"name": "analyze_text"}},
    )
    tool_calls = response.choices[0].message.tool_calls
    if not tool_calls:
        raise RuntimeError("OpenAI response did not include an analyze_text tool call")
    arguments = json.loads(tool_calls[0].function.arguments)
    usage = {
        "prompt_tokens": getattr(response.usage, "prompt_tokens", None),
        "completion_tokens": getattr(response.usage, "completion_tokens", None),
        "model": response.model,
    }
    return arguments, usage


def update_sqlite_processed(conn: sqlite3.Connection, row: sqlite3.Row, full_text: str, arguments: dict, usage: dict) -> None:
    conn.execute(
        """
        UPDATE fulltext_articles
        SET
            extracted_text = ?,
            ai_analysis_status = 'processed',
            ai_caucasian = ?,
            ai_white = ?,
            ai_european = ?,
            ai_european_phrase_used = ?,
            ai_other = ?,
            ai_other_phrase_used = ?,
            ai_model = ?,
            ai_prompt_tokens = ?,
            ai_completion_tokens = ?,
            ai_error = NULL,
            ai_processed_at = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE batch_slug = ? AND article_id = ?
        """,
        (
            full_text,
            sqlite_bool(bool(arguments.get("caucasian", False))),
            sqlite_bool(bool(arguments.get("white", False))),
            sqlite_bool(bool(arguments.get("european", False))),
            sanitize_string(arguments.get("european_phrase_used", "")),
            sqlite_bool(bool(arguments.get("other", False))),
            sanitize_string(arguments.get("other_phrase_used", "")),
            sanitize_string(usage.get("model", "")),
            usage.get("prompt_tokens"),
            usage.get("completion_tokens"),
            now_utc(),
            row["batch_slug"],
            row["article_id"],
        ),
    )
    conn.commit()


def update_sqlite_failed(conn: sqlite3.Connection, row: sqlite3.Row, error: Exception) -> None:
    conn.execute(
        """
        UPDATE fulltext_articles
        SET ai_analysis_status = 'failed',
            ai_error = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE batch_slug = ? AND article_id = ?
        """,
        (sanitize_string(str(error))[:2000], row["batch_slug"], row["article_id"]),
    )
    conn.commit()


def load_pg_processed_result(cur, row: sqlite3.Row) -> dict | None:
    cur.execute(
        """
        SELECT
            a.ai_analysis_status,
            a.ai_caucasian,
            a.ai_white,
            a.ai_european,
            a.ai_european_phrase_used,
            a.ai_other,
            a.ai_other_phrase_used,
            a.ai_model,
            a.ai_prompt_tokens,
            a.ai_completion_tokens,
            a.ai_error,
            a.ai_processed_at,
            a.extracted_text,
            a.fulltext_path
        FROM languageingenetics.fulltext_audit_articles a
        JOIN languageingenetics.fulltext_audit_batches b
          ON b.id = a.batch_id
        WHERE b.slug = %s
          AND a.article_id = %s
          AND a.ai_analysis_status = 'processed'
        """,
        (row["batch_slug"], row["article_id"]),
    )
    result = cur.fetchone()
    return dict(result) if result else None


def same_processed_source(row: sqlite3.Row, result: dict) -> bool:
    local_text = sanitize_string(row["extracted_text"]).strip()
    processed_text = sanitize_string(result.get("extracted_text", "")).strip()
    if local_text:
        return local_text == processed_text
    return sanitize_string(row["fulltext_path"]) == sanitize_string(result.get("fulltext_path"))


def update_sqlite_from_pg_processed(conn: sqlite3.Connection, row: sqlite3.Row, result: dict) -> None:
    conn.execute(
        """
        UPDATE fulltext_articles
        SET
            extracted_text = COALESCE(NULLIF(?, ''), extracted_text),
            ai_analysis_status = 'processed',
            ai_caucasian = ?,
            ai_white = ?,
            ai_european = ?,
            ai_european_phrase_used = ?,
            ai_other = ?,
            ai_other_phrase_used = ?,
            ai_model = ?,
            ai_prompt_tokens = ?,
            ai_completion_tokens = ?,
            ai_error = ?,
            ai_processed_at = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE batch_slug = ? AND article_id = ?
        """,
        (
            sanitize_string(result.get("extracted_text", "")),
            sqlite_bool(result.get("ai_caucasian")),
            sqlite_bool(result.get("ai_white")),
            sqlite_bool(result.get("ai_european")),
            sanitize_string(result.get("ai_european_phrase_used", "")),
            sqlite_bool(result.get("ai_other")),
            sanitize_string(result.get("ai_other_phrase_used", "")),
            sanitize_string(result.get("ai_model", "")),
            result.get("ai_prompt_tokens"),
            result.get("ai_completion_tokens"),
            sanitize_string(result.get("ai_error", "")),
            str(result.get("ai_processed_at") or ""),
            row["batch_slug"],
            row["article_id"],
        ),
    )
    conn.commit()


def update_pg_processed(cur, row: sqlite3.Row, full_text: str, arguments: dict, usage: dict) -> None:
    cur.execute(
        """
        UPDATE languageingenetics.fulltext_audit_articles a
        SET
            extracted_text = %s,
            ai_analysis_status = 'processed',
            ai_caucasian = %s,
            ai_white = %s,
            ai_european = %s,
            ai_european_phrase_used = %s,
            ai_other = %s,
            ai_other_phrase_used = %s,
            ai_model = %s,
            ai_prompt_tokens = %s,
            ai_completion_tokens = %s,
            ai_error = NULL,
            ai_processed_at = CURRENT_TIMESTAMP
        FROM languageingenetics.fulltext_audit_batches b
        WHERE a.batch_id = b.id
          AND b.slug = %s
          AND a.article_id = %s
        """,
        (
            full_text,
            bool(arguments.get("caucasian", False)),
            bool(arguments.get("white", False)),
            bool(arguments.get("european", False)),
            sanitize_string(arguments.get("european_phrase_used", "")),
            bool(arguments.get("other", False)),
            sanitize_string(arguments.get("other_phrase_used", "")),
            sanitize_string(usage.get("model", "")),
            usage.get("prompt_tokens"),
            usage.get("completion_tokens"),
            row["batch_slug"],
            row["article_id"],
        ),
    )
    if cur.rowcount != 1:
        raise RuntimeError(f"updated {cur.rowcount} PostgreSQL rows for {row['batch_slug']} article {row['article_id']}")


def update_pg_failed(cur, row: sqlite3.Row, error: Exception) -> None:
    cur.execute(
        """
        UPDATE languageingenetics.fulltext_audit_articles a
        SET ai_analysis_status = 'failed',
            ai_error = %s
        FROM languageingenetics.fulltext_audit_batches b
        WHERE a.batch_id = b.id
          AND b.slug = %s
          AND a.article_id = %s
        """,
        (sanitize_string(str(error))[:2000], row["batch_slug"], row["article_id"]),
    )


def queued_rows(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            batch_slug,
            article_id,
            title,
            abstract,
            fulltext_path,
            extracted_text
        FROM fulltext_articles
        WHERE ai_analysis_status = 'queued'
          AND (
              TRIM(COALESCE(extracted_text, '')) <> ''
              OR TRIM(COALESCE(fulltext_path, '')) <> ''
          )
        ORDER BY datetime(updated_at), batch_slug, article_id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def main() -> int:
    parser = argparse.ArgumentParser(description="Process queued full-text AI uploads")
    parser.add_argument("--sqlite-db", default="../audit/review_data/lig_audit.db", help="SQLite upload queue database path")
    parser.add_argument(
        "--openai-api-key",
        default=os.environ.get("OPENAI_API_KEY_FILE", os.path.expanduser("~/.openai.lig.key")),
        help="Path to the OpenAI API key file",
    )
    parser.add_argument("--model", default="gpt-5-mini", help="OpenAI model for full-text analysis")
    parser.add_argument("--limit", type=int, default=10, help="Maximum queued uploads to process")
    parser.add_argument("--max-chars", type=int, default=120000, help="Maximum article-text characters to send")
    parser.add_argument("--dry-run", action="store_true", help="List queued rows without calling OpenAI or updating state")
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_db).resolve()
    if not sqlite_path.exists():
        print(f"SQLite audit database not found: {sqlite_path}")
        return 0

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    ensure_sqlite_ai_columns(sqlite_conn)
    rows = queued_rows(sqlite_conn, args.limit)
    if not rows:
        print("No queued full-text uploads found.")
        sqlite_conn.close()
        return 0

    if args.dry_run:
        for row in rows:
            print(f"{row['batch_slug']} article {row['article_id']}: {row['title']}")
        sqlite_conn.close()
        return 0

    api_key = Path(args.openai_api_key).read_text().strip()
    client = openai.OpenAI(api_key=api_key)

    pg_conn = get_pg_connection()
    pg_conn.autocommit = False
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    ensure_pg_schema(pg_cur)
    pg_conn.commit()

    processed = 0
    failed = 0
    try:
        for row in rows:
            label = f"{row['batch_slug']} article {row['article_id']}"
            try:
                existing = load_pg_processed_result(pg_cur, row)
                if existing and same_processed_source(row, existing):
                    update_sqlite_from_pg_processed(sqlite_conn, row, existing)
                    print(f"Already processed in PostgreSQL; refreshed local SQLite for {label}")
                    continue
                full_text = full_text_for_row(sqlite_path, row)
                if not full_text:
                    raise RuntimeError("extracted full text was empty")
                arguments, usage = analyze_article(client, args.model, row, full_text, args.max_chars)
                update_sqlite_processed(sqlite_conn, row, full_text, arguments, usage)
                update_pg_processed(pg_cur, row, full_text, arguments, usage)
                pg_conn.commit()
                processed += 1
                print(f"Processed {label}")
            except Exception as exc:
                update_sqlite_failed(sqlite_conn, row, exc)
                try:
                    update_pg_failed(pg_cur, row, exc)
                    pg_conn.commit()
                except Exception as pg_exc:
                    pg_conn.rollback()
                    print(f"Failed to record PostgreSQL failure for {label}: {pg_exc}", file=sys.stderr)
                failed += 1
                print(f"Failed {label}: {exc}", file=sys.stderr)
    finally:
        pg_cur.close()
        pg_conn.close()
        sqlite_conn.close()

    print(f"Full-text AI processing complete: {processed} processed, {failed} failed.")
    return 1 if failed and not processed else 0


if __name__ == "__main__":
    raise SystemExit(main())
