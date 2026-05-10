#!/usr/bin/env python3
"""
Import full-text audit review state from the merah SQLite database into PostgreSQL.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path

import psycopg2
import psycopg2.extras


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


def sqlite_table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def nullable_bool(value):
    if value is None:
        return None
    return bool(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Import full-text audit reviews from SQLite into PostgreSQL")
    parser.add_argument("--sqlite-db", default="../audit/review_data/lig_audit.db", help="SQLite database path")
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_db).resolve()
    if not sqlite_path.exists():
        print(f"SQLite audit database not found: {sqlite_path}")
        return 0

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    if not sqlite_table_exists(sqlite_conn, "fulltext_batches"):
        print(f"SQLite database has no fulltext_batches table: {sqlite_path}")
        sqlite_conn.close()
        return 0

    pg_conn = get_pg_connection()
    pg_conn.autocommit = False
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    ensure_pg_schema(pg_cur)

    imported_batches = 0
    imported_articles = 0
    imported_reviews = 0
    cleared_reviews = 0

    try:
        sqlite_batches = sqlite_conn.execute(
            """
            SELECT
                batch_slug,
                seed,
                sample_size,
                created_at,
                created_by,
                source_filter,
                notes
            FROM fulltext_batches
            ORDER BY created_at, batch_slug
            """
        ).fetchall()

        batch_id_by_slug: dict[str, int] = {}
        for batch in sqlite_batches:
            pg_cur.execute(
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
                ON CONFLICT (slug) DO UPDATE SET
                    seed = EXCLUDED.seed,
                    sample_size = EXCLUDED.sample_size,
                    created_at = EXCLUDED.created_at,
                    created_by = EXCLUDED.created_by,
                    source_filter = EXCLUDED.source_filter,
                    notes = EXCLUDED.notes
                RETURNING id
                """,
                (
                    batch["batch_slug"],
                    batch["seed"],
                    batch["sample_size"],
                    batch["created_at"],
                    batch["created_by"],
                    batch["source_filter"],
                    batch["notes"],
                ),
            )
            batch_id_by_slug[str(batch["batch_slug"])] = int(pg_cur.fetchone()["id"])
            imported_batches += 1

        sqlite_articles = sqlite_conn.execute(
            """
            SELECT
                batch_slug,
                article_id,
                work_id,
                work_version_id,
                doi,
                journal_name,
                pub_year,
                title,
                abstract,
                fulltext_status,
                fulltext_source,
                fulltext_path,
                extracted_text,
                terminology_present,
                caucasian_present,
                white_present,
                european_present,
                other_present,
                quoted_evidence,
                reviewer_username,
                review_notes,
                reviewed_at,
                updated_at
            FROM fulltext_articles
            ORDER BY batch_slug, article_id
            """
        ).fetchall()

        for row in sqlite_articles:
            batch_id = batch_id_by_slug[str(row["batch_slug"])]
            pg_cur.execute(
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
                    fulltext_status,
                    fulltext_source,
                    fulltext_path,
                    extracted_text
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, article_id) DO UPDATE SET
                    work_id = EXCLUDED.work_id,
                    work_version_id = EXCLUDED.work_version_id,
                    doi = EXCLUDED.doi,
                    journal_name = EXCLUDED.journal_name,
                    pub_year = EXCLUDED.pub_year,
                    title = EXCLUDED.title,
                    abstract = EXCLUDED.abstract,
                    fulltext_status = EXCLUDED.fulltext_status,
                    fulltext_source = EXCLUDED.fulltext_source,
                    fulltext_path = EXCLUDED.fulltext_path,
                    extracted_text = EXCLUDED.extracted_text
                RETURNING id
                """,
                (
                    batch_id,
                    row["article_id"],
                    row["work_id"],
                    row["work_version_id"],
                    row["doi"],
                    row["journal_name"],
                    row["pub_year"],
                    row["title"],
                    row["abstract"],
                    row["fulltext_status"],
                    row["fulltext_source"],
                    row["fulltext_path"],
                    row["extracted_text"],
                ),
            )
            sample_article_id = int(pg_cur.fetchone()["id"])
            imported_articles += 1

            if row["terminology_present"] is None:
                pg_cur.execute(
                    """
                    DELETE FROM languageingenetics.fulltext_audit_reviews
                    WHERE sample_article_id = %s
                    """,
                    (sample_article_id,),
                )
                if pg_cur.rowcount:
                    cleared_reviews += 1
                continue

            pg_cur.execute(
                """
                INSERT INTO languageingenetics.fulltext_audit_reviews (
                    sample_article_id,
                    terminology_present,
                    caucasian_present,
                    white_present,
                    european_present,
                    other_present,
                    quoted_evidence,
                    reviewer_username,
                    review_notes,
                    reviewed_at,
                    updated_at,
                    source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'merah_audit_sqlite')
                ON CONFLICT (sample_article_id) DO UPDATE SET
                    terminology_present = EXCLUDED.terminology_present,
                    caucasian_present = EXCLUDED.caucasian_present,
                    white_present = EXCLUDED.white_present,
                    european_present = EXCLUDED.european_present,
                    other_present = EXCLUDED.other_present,
                    quoted_evidence = EXCLUDED.quoted_evidence,
                    reviewer_username = EXCLUDED.reviewer_username,
                    review_notes = EXCLUDED.review_notes,
                    reviewed_at = EXCLUDED.reviewed_at,
                    updated_at = EXCLUDED.updated_at,
                    source = EXCLUDED.source
                """,
                (
                    sample_article_id,
                    nullable_bool(row["terminology_present"]),
                    nullable_bool(row["caucasian_present"]),
                    nullable_bool(row["white_present"]),
                    nullable_bool(row["european_present"]),
                    nullable_bool(row["other_present"]),
                    row["quoted_evidence"],
                    row["reviewer_username"],
                    row["review_notes"],
                    row["reviewed_at"],
                    row["updated_at"],
                ),
            )
            imported_reviews += 1

        pg_conn.commit()
    finally:
        pg_cur.close()
        pg_conn.close()
        sqlite_conn.close()

    print(
        f"Imported {imported_batches} full-text batches, {imported_articles} articles, "
        f"upserted {imported_reviews} reviews, cleared {cleared_reviews} PostgreSQL reviews from {sqlite_path}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
