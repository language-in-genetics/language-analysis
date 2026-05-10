#!/usr/bin/env python3
"""
Create a reproducible full-text human-audit batch.

This seeds:
1. PostgreSQL under languageingenetics.fulltext_audit_*
2. The SQLite database used by the merah CGI workflow
"""

from __future__ import annotations

import argparse
import os
import random
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras


VALID_FULLTEXT_STATUSES = {
    "pending_fetch",
    "available",
    "needs_manual",
    "unavailable",
    "extraction_failed",
}


@dataclass(frozen=True)
class FulltextBatchConfig:
    slug: str
    seed: int
    sample_size: int
    created_at: str
    created_by: str
    source_filter: str
    notes: str | None


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


def ensure_sqlite_schema(conn: sqlite3.Connection) -> None:
    schema_path = Path(__file__).resolve().parents[1] / "audit_cgi" / "init_schema.sql"
    conn.executescript(schema_path.read_text())
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


def build_filters(args: argparse.Namespace) -> tuple[list[str], list[object]]:
    clauses = ["is_processed = true"]
    params: list[object] = []

    if args.min_year is not None:
        clauses.append("pub_year >= %s")
        params.append(args.min_year)
    if args.max_year is not None:
        clauses.append("pub_year <= %s")
        params.append(args.max_year)
    if args.journal:
        clauses.append("journal_name = %s")
        params.append(args.journal)
    if args.require_abstract:
        clauses.append("has_abstract = true")

    return clauses, params


def fetch_candidate_ids(cur, args: argparse.Namespace, clauses: list[str], params: list[object]) -> list[int]:
    if not args.journal and not args.require_abstract:
        files_clauses = ["processed = true", "article_id IS NOT NULL"]
        files_params: list[object] = []
        if args.min_year is not None:
            files_clauses.append("pub_year >= %s")
            files_params.append(args.min_year)
        if args.max_year is not None:
            files_clauses.append("pub_year <= %s")
            files_params.append(args.max_year)
        cur.execute(
            f"""
            SELECT article_id
            FROM languageingenetics.files
            WHERE {' AND '.join(files_clauses)}
            ORDER BY article_id
            """,
            files_params,
        )
    else:
        cur.execute(
            f"""
            SELECT article_id
            FROM languageingenetics.focused_journals_view
            WHERE {' AND '.join(clauses)}
            ORDER BY article_id
            """,
            params,
        )
    return [int(row["article_id"]) for row in cur.fetchall()]


def choose_ids(candidate_ids: list[int], sample_size: int, seed: int) -> list[int]:
    if len(candidate_ids) < sample_size:
        raise ValueError(
            f"Requested sample size {sample_size}, but only found {len(candidate_ids)} candidates."
        )
    rng = random.Random(seed)
    return sorted(rng.sample(candidate_ids, sample_size))


def fetch_details(cur, article_ids: list[int]) -> list[dict]:
    cur.execute(
        """
        SELECT
            v.article_id,
            f.work_id,
            f.work_version_id,
            v.doi,
            v.journal_name,
            v.pub_year,
            v.title,
            v.abstract
        FROM languageingenetics.focused_journals_view v
        LEFT JOIN languageingenetics.files f
          ON f.article_id = v.article_id
        WHERE v.article_id = ANY(%s)
        """,
        (article_ids,),
    )
    by_id = {int(row["article_id"]): row for row in cur.fetchall()}
    missing = [article_id for article_id in article_ids if article_id not in by_id]
    if missing:
        raise RuntimeError(f"Failed to load details for article IDs: {missing[:10]}")
    return [by_id[article_id] for article_id in article_ids]


def batch_exists_pg(cur, slug: str) -> bool:
    cur.execute(
        "SELECT 1 FROM languageingenetics.fulltext_audit_batches WHERE slug = %s LIMIT 1",
        (slug,),
    )
    return cur.fetchone() is not None


def batch_exists_sqlite(conn: sqlite3.Connection, slug: str) -> bool:
    row = conn.execute("SELECT 1 FROM fulltext_batches WHERE batch_slug = ?", (slug,)).fetchone()
    return row is not None


def insert_pg_batch(cur, cfg: FulltextBatchConfig) -> int:
    cur.execute(
        """
        INSERT INTO languageingenetics.fulltext_audit_batches (
            slug,
            seed,
            sample_size,
            created_at,
            created_by,
            source_filter,
            notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            cfg.slug,
            cfg.seed,
            cfg.sample_size,
            cfg.created_at,
            cfg.created_by,
            cfg.source_filter,
            cfg.notes,
        ),
    )
    return int(cur.fetchone()["id"])


def insert_pg_articles(cur, batch_id: int, rows: list[dict], fulltext_status: str) -> None:
    psycopg2.extras.execute_batch(
        cur,
        """
        INSERT INTO languageingenetics.fulltext_audit_articles (
            batch_id,
            article_id,
            work_id,
            work_version_id,
            doi,
            journal_name,
            pub_year,
            title,
            abstract,
            fulltext_status
        )
        VALUES (
            %(batch_id)s,
            %(article_id)s,
            %(work_id)s,
            %(work_version_id)s,
            %(doi)s,
            %(journal_name)s,
            %(pub_year)s,
            %(title)s,
            %(abstract)s,
            %(fulltext_status)s
        )
        """,
        [
            {
                "batch_id": batch_id,
                "article_id": int(row["article_id"]),
                "work_id": row["work_id"],
                "work_version_id": row["work_version_id"],
                "doi": row["doi"],
                "journal_name": row["journal_name"],
                "pub_year": row["pub_year"],
                "title": row["title"],
                "abstract": row["abstract"],
                "fulltext_status": fulltext_status,
            }
            for row in rows
        ],
        page_size=200,
    )


def insert_sqlite_batch(conn: sqlite3.Connection, cfg: FulltextBatchConfig) -> None:
    conn.execute(
        """
        INSERT INTO fulltext_batches (
            batch_slug,
            seed,
            sample_size,
            created_at,
            created_by,
            source_filter,
            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cfg.slug,
            cfg.seed,
            cfg.sample_size,
            cfg.created_at,
            cfg.created_by,
            cfg.source_filter,
            cfg.notes,
        ),
    )


def insert_sqlite_articles(conn: sqlite3.Connection, cfg: FulltextBatchConfig, rows: list[dict], fulltext_status: str) -> None:
    conn.executemany(
        """
        INSERT INTO fulltext_articles (
            batch_slug,
            article_id,
            work_id,
            work_version_id,
            doi,
            journal_name,
            pub_year,
            title,
            abstract,
            fulltext_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                cfg.slug,
                int(row["article_id"]),
                row["work_id"],
                row["work_version_id"],
                row["doi"],
                row["journal_name"],
                row["pub_year"],
                row["title"],
                row["abstract"],
                fulltext_status,
            )
            for row in rows
        ],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a reproducible full-text human-audit batch")
    parser.add_argument("--sqlite-db", default="../audit/review_data/lig_audit.db", help="SQLite database path to seed/update")
    parser.add_argument("--batch-slug", help="Batch slug to create (default: fulltext-YYYYMMDD-seedN)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--sample-size", type=int, default=100, help="Number of articles to sample")
    parser.add_argument("--min-year", type=int, help="Minimum publication year")
    parser.add_argument("--max-year", type=int, default=2025, help="Maximum publication year")
    parser.add_argument("--journal", help="Restrict sampling to one journal")
    parser.add_argument("--require-abstract", action="store_true", help="Only sample articles with abstracts")
    parser.add_argument(
        "--fulltext-status",
        default="pending_fetch",
        choices=sorted(VALID_FULLTEXT_STATUSES),
        help="Initial full-text acquisition status",
    )
    parser.add_argument("--created-by", default=os.environ.get("USER", "unknown"), help="Batch creator label")
    parser.add_argument("--notes", help="Optional batch notes")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    slug = args.batch_slug or f"fulltext-{now.strftime('%Y%m%d')}-seed{args.seed}"
    source_parts = ["processed=true", "scope=focused_journals_view"]
    if args.min_year is not None:
        source_parts.append(f"min_year={args.min_year}")
    if args.max_year is not None:
        source_parts.append(f"max_year={args.max_year}")
    if args.journal:
        source_parts.append(f"journal={args.journal}")
    if args.require_abstract:
        source_parts.append("require_abstract=true")
    source_parts.append(f"sample_size={args.sample_size}")
    source_parts.append(f"initial_fulltext_status={args.fulltext_status}")
    cfg = FulltextBatchConfig(
        slug=slug,
        seed=args.seed,
        sample_size=args.sample_size,
        created_at=now.isoformat(),
        created_by=args.created_by,
        source_filter=", ".join(source_parts),
        notes=args.notes,
    )

    sqlite_path = Path(args.sqlite_db).resolve()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    ensure_sqlite_schema(sqlite_conn)

    pg_conn = get_pg_connection()
    pg_conn.autocommit = False
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    ensure_pg_schema(pg_cur)

    try:
        if batch_exists_sqlite(sqlite_conn, cfg.slug):
            raise RuntimeError(f"SQLite already contains full-text batch {cfg.slug}")
        if batch_exists_pg(pg_cur, cfg.slug):
            raise RuntimeError(f"PostgreSQL already contains full-text batch {cfg.slug}")

        clauses, params = build_filters(args)
        candidate_ids = fetch_candidate_ids(pg_cur, args, clauses, params)
        chosen_ids = choose_ids(candidate_ids, args.sample_size, args.seed)
        rows = fetch_details(pg_cur, chosen_ids)

        batch_id = insert_pg_batch(pg_cur, cfg)
        insert_pg_articles(pg_cur, batch_id, rows, args.fulltext_status)
        insert_sqlite_batch(sqlite_conn, cfg)
        insert_sqlite_articles(sqlite_conn, cfg, rows, args.fulltext_status)

        pg_conn.commit()
        sqlite_conn.commit()
    except Exception:
        pg_conn.rollback()
        sqlite_conn.rollback()
        raise
    finally:
        pg_cur.close()
        pg_conn.close()
        sqlite_conn.close()

    print(f"Created full-text AI processing batch {cfg.slug}: {len(rows)} articles from {len(candidate_ids)} candidates.")
    print(f"SQLite DB: {sqlite_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
