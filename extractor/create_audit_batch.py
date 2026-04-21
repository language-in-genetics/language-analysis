#!/usr/bin/env python3
"""
Create a reproducible human-audit batch for positive and negative classifier hits.

This writes the sampled batch into:
1. PostgreSQL under languageingenetics.audit_*
2. A SQLite database intended for the CGI workflow on merah
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


SQLITE_SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS audit_batches (
    sample_batch TEXT PRIMARY KEY,
    seed INTEGER NOT NULL,
    positive_sample_size INTEGER NOT NULL,
    negative_sample_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT,
    source_filter TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS audit_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_batch TEXT NOT NULL REFERENCES audit_batches(sample_batch) ON DELETE CASCADE,
    sample_group TEXT NOT NULL CHECK (sample_group IN ('positive', 'negative')),
    article_id INTEGER NOT NULL,
    predicted_positive INTEGER NOT NULL CHECK (predicted_positive IN (0, 1)),
    doi TEXT,
    journal_name TEXT,
    pub_year INTEGER,
    title TEXT,
    abstract TEXT,
    classifier_caucasian INTEGER NOT NULL DEFAULT 0,
    classifier_white INTEGER NOT NULL DEFAULT 0,
    classifier_european INTEGER NOT NULL DEFAULT 0,
    classifier_other INTEGER NOT NULL DEFAULT 0,
    classifier_european_phrase_used TEXT,
    classifier_other_phrase_used TEXT,
    human_positive INTEGER CHECK (human_positive IN (0, 1)),
    reviewer_username TEXT,
    review_notes TEXT,
    reviewed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sample_batch, article_id)
);

CREATE INDEX IF NOT EXISTS audit_articles_batch_group_idx
    ON audit_articles (sample_batch, sample_group, article_id);

CREATE INDEX IF NOT EXISTS audit_articles_reviewed_idx
    ON audit_articles (sample_batch, human_positive, reviewer_username);
"""


@dataclass(frozen=True)
class BatchConfig:
    slug: str
    seed: int
    positive_size: int
    negative_size: int
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
    conn.executescript(SQLITE_SCHEMA)


def build_filters(args: argparse.Namespace) -> tuple[str, list]:
    clauses = []
    params: list[object] = []

    if args.min_year is not None:
        clauses.append("((regexp_replace(regexp_replace(r.filesrc, E'\\n', ' ', 'g'), E'\\t', '    ', 'g')::jsonb -> 'published' -> 'date-parts' -> 0 ->> 0)::integer) >= %s")
        params.append(args.min_year)

    if args.max_year is not None:
        clauses.append("((regexp_replace(regexp_replace(r.filesrc, E'\\n', ' ', 'g'), E'\\t', '    ', 'g')::jsonb -> 'published' -> 'date-parts' -> 0 ->> 0)::integer) <= %s")
        params.append(args.max_year)

    if args.journal:
        clauses.append("j.name = %s")
        params.append(args.journal)

    return " AND ".join(clauses), params


def fetch_candidate_ids(cur, *, positive: bool, filter_sql: str, filter_params: list[object]) -> list[int]:
    positive_clause = "(COALESCE(f.caucasian, false) OR COALESCE(f.white, false) OR COALESCE(f.european, false) OR COALESCE(f.other, false))"
    negative_clause = "(NOT COALESCE(f.caucasian, false) AND NOT COALESCE(f.white, false) AND NOT COALESCE(f.european, false) AND NOT COALESCE(f.other, false))"
    polarity_clause = positive_clause if positive else negative_clause

    if filter_sql:
        query = f"""
            SELECT f.article_id
            FROM languageingenetics.files f
            JOIN public.raw_text_data r
              ON r.id = f.article_id
            JOIN languageingenetics.journals j
              ON (regexp_replace(regexp_replace(r.filesrc, E'\\n', ' ', 'g'), E'\\t', '    ', 'g')::jsonb -> 'container-title' ->> 0) = j.name
            WHERE j.enabled = true
              AND f.processed = true
              AND {filter_sql}
              AND {polarity_clause}
            ORDER BY f.article_id
        """
        cur.execute(query, filter_params)
    else:
        query = f"""
            SELECT f.article_id
            FROM languageingenetics.files f
            WHERE f.processed = true
              AND {polarity_clause}
            ORDER BY f.article_id
        """
        cur.execute(query)
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
            f.article_id,
            (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'DOI') AS doi,
            j.name AS journal_name,
            ((regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'published' -> 'date-parts' -> 0 ->> 0)::integer) AS pub_year,
            (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'title') AS title,
            (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'abstract') AS abstract,
            COALESCE(f.caucasian, false) AS caucasian,
            COALESCE(f.white, false) AS white,
            COALESCE(f.european, false) AS european,
            COALESCE(f.other, false) AS other,
            f.european_phrase_used,
            f.other_phrase_used
        FROM languageingenetics.files f
        JOIN public.raw_text_data r
          ON r.id = f.article_id
        JOIN languageingenetics.journals j
          ON (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'container-title' ->> 0) = j.name
        WHERE f.article_id = ANY(%s)
        """,
        (article_ids,),
    )
    by_id = {int(row["article_id"]): row for row in cur.fetchall()}
    missing = [article_id for article_id in article_ids if article_id not in by_id]
    if missing:
        raise RuntimeError(f"Failed to load details for article IDs: {missing[:10]}")
    return [by_id[article_id] for article_id in article_ids]


def insert_pg_batch(cur, cfg: BatchConfig) -> int:
    cur.execute(
        """
        INSERT INTO languageingenetics.audit_sample_batches (
            slug,
            seed,
            positive_sample_size,
            negative_sample_size,
            created_at,
            created_by,
            source_filter,
            notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            cfg.slug,
            cfg.seed,
            cfg.positive_size,
            cfg.negative_size,
            cfg.created_at,
            cfg.created_by,
            cfg.source_filter,
            cfg.notes,
        ),
    )
    return int(cur.fetchone()["id"])


def insert_pg_articles(cur, batch_id: int, sample_group: str, predicted_positive: bool, rows: list[dict]) -> None:
    psycopg2.extras.execute_batch(
        cur,
        """
        INSERT INTO languageingenetics.audit_sample_articles (
            batch_id,
            article_id,
            sample_group,
            predicted_positive,
            doi,
            journal_name,
            pub_year,
            title,
            abstract,
            classifier_caucasian,
            classifier_white,
            classifier_european,
            classifier_other,
            classifier_european_phrase_used,
            classifier_other_phrase_used
        )
        VALUES (
            %(batch_id)s,
            %(article_id)s,
            %(sample_group)s,
            %(predicted_positive)s,
            %(doi)s,
            %(journal_name)s,
            %(pub_year)s,
            %(title)s,
            %(abstract)s,
            %(caucasian)s,
            %(white)s,
            %(european)s,
            %(other)s,
            %(european_phrase_used)s,
            %(other_phrase_used)s
        )
        """,
        [
            {
                "batch_id": batch_id,
                "article_id": int(row["article_id"]),
                "sample_group": sample_group,
                "predicted_positive": predicted_positive,
                "doi": row["doi"],
                "journal_name": row["journal_name"],
                "pub_year": row["pub_year"],
                "title": row["title"],
                "abstract": row["abstract"],
                "caucasian": bool(row["caucasian"]),
                "white": bool(row["white"]),
                "european": bool(row["european"]),
                "other": bool(row["other"]),
                "european_phrase_used": row["european_phrase_used"],
                "other_phrase_used": row["other_phrase_used"],
            }
            for row in rows
        ],
        page_size=200,
    )


def insert_sqlite_batch(conn: sqlite3.Connection, cfg: BatchConfig) -> None:
    conn.execute(
        """
        INSERT INTO audit_batches (
            sample_batch,
            seed,
            positive_sample_size,
            negative_sample_size,
            created_at,
            created_by,
            source_filter,
            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cfg.slug,
            cfg.seed,
            cfg.positive_size,
            cfg.negative_size,
            cfg.created_at,
            cfg.created_by,
            cfg.source_filter,
            cfg.notes,
        ),
    )


def insert_sqlite_articles(conn: sqlite3.Connection, sample_group: str, predicted_positive: bool, cfg: BatchConfig, rows: list[dict]) -> None:
    conn.executemany(
        """
        INSERT INTO audit_articles (
            sample_batch,
            sample_group,
            article_id,
            predicted_positive,
            doi,
            journal_name,
            pub_year,
            title,
            abstract,
            classifier_caucasian,
            classifier_white,
            classifier_european,
            classifier_other,
            classifier_european_phrase_used,
            classifier_other_phrase_used
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                cfg.slug,
                sample_group,
                int(row["article_id"]),
                1 if predicted_positive else 0,
                row["doi"],
                row["journal_name"],
                row["pub_year"],
                row["title"],
                row["abstract"],
                1 if row["caucasian"] else 0,
                1 if row["white"] else 0,
                1 if row["european"] else 0,
                1 if row["other"] else 0,
                row["european_phrase_used"],
                row["other_phrase_used"],
            )
            for row in rows
        ],
    )


def batch_exists_sqlite(conn: sqlite3.Connection, slug: str) -> bool:
    row = conn.execute("SELECT 1 FROM audit_batches WHERE sample_batch = ?", (slug,)).fetchone()
    return row is not None


def batch_exists_pg(cur, slug: str) -> bool:
    cur.execute(
        "SELECT 1 FROM languageingenetics.audit_sample_batches WHERE slug = %s LIMIT 1",
        (slug,),
    )
    return cur.fetchone() is not None


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a reproducible human-audit batch")
    parser.add_argument("--sqlite-db", default="../audit/review_data/lig_audit.db", help="SQLite database path to seed/update")
    parser.add_argument("--batch-slug", help="Batch slug to create (default: audit-YYYYMMDD-seedN)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--positive-size", type=int, default=100, help="Positive-hit sample size")
    parser.add_argument("--negative-size", type=int, default=100, help="Negative-hit sample size")
    parser.add_argument("--min-year", type=int, help="Minimum publication year")
    parser.add_argument("--max-year", type=int, help="Maximum publication year")
    parser.add_argument("--journal", help="Restrict sampling to one journal")
    parser.add_argument("--created-by", default=os.environ.get("USER", "unknown"), help="Batch creator label")
    parser.add_argument("--notes", help="Optional batch notes")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    slug = args.batch_slug or f"audit-{now.strftime('%Y%m%d')}-seed{args.seed}"
    source_parts = ["processed=true", "scope=focused_journals_view"]
    if args.min_year is not None:
        source_parts.append(f"min_year={args.min_year}")
    if args.max_year is not None:
        source_parts.append(f"max_year={args.max_year}")
    if args.journal:
        source_parts.append(f"journal={args.journal}")
    cfg = BatchConfig(
        slug=slug,
        seed=args.seed,
        positive_size=args.positive_size,
        negative_size=args.negative_size,
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
            raise RuntimeError(f"SQLite already contains batch {cfg.slug}")
        if batch_exists_pg(pg_cur, cfg.slug):
            raise RuntimeError(f"PostgreSQL already contains batch {cfg.slug}")

        filter_sql, filter_params = build_filters(args)

        positive_ids = fetch_candidate_ids(pg_cur, positive=True, filter_sql=filter_sql, filter_params=filter_params)
        negative_ids = fetch_candidate_ids(pg_cur, positive=False, filter_sql=filter_sql, filter_params=filter_params)

        chosen_positive_ids = choose_ids(positive_ids, args.positive_size, args.seed)
        chosen_negative_ids = choose_ids(negative_ids, args.negative_size, args.seed + 1)

        positive_rows = fetch_details(pg_cur, chosen_positive_ids)
        negative_rows = fetch_details(pg_cur, chosen_negative_ids)

        batch_id = insert_pg_batch(pg_cur, cfg)
        insert_pg_articles(pg_cur, batch_id, "positive", True, positive_rows)
        insert_pg_articles(pg_cur, batch_id, "negative", False, negative_rows)

        insert_sqlite_batch(sqlite_conn, cfg)
        insert_sqlite_articles(sqlite_conn, "positive", True, cfg, positive_rows)
        insert_sqlite_articles(sqlite_conn, "negative", False, cfg, negative_rows)

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

    print(
        f"Created audit batch {cfg.slug}: "
        f"{args.positive_size} positive and {args.negative_size} negative samples."
    )
    print(f"SQLite DB: {sqlite_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
