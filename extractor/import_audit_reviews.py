#!/usr/bin/env python3
"""
Import the merah audit SQLite database back into PostgreSQL.
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Import audit reviews from SQLite into PostgreSQL")
    parser.add_argument("--sqlite-db", default="../audit/review_data/lig_audit.db", help="SQLite database path")
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_db).resolve()
    if not sqlite_path.exists():
        print(f"SQLite audit database not found: {sqlite_path}")
        return 0

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = get_pg_connection()
    pg_conn.autocommit = False
    pg_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    ensure_pg_schema(pg_cur)

    imported_batches = 0
    imported_reviews = 0
    cleared_reviews = 0

    try:
        sqlite_batches = sqlite_conn.execute(
            """
            SELECT
                sample_batch,
                seed,
                matched_label_sample_size,
                none_of_these_labels_sample_size,
                created_at,
                created_by,
                source_filter,
                notes
            FROM audit_batches
            ORDER BY created_at, sample_batch
            """
        ).fetchall()

        batch_id_by_slug: dict[str, int] = {}
        for batch in sqlite_batches:
            pg_cur.execute(
                """
                INSERT INTO languageingenetics.audit_sample_batches (
                    slug,
                    seed,
                    matched_label_sample_size,
                    none_of_these_labels_sample_size,
                    created_at,
                    created_by,
                    source_filter,
                    notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (slug) DO UPDATE SET
                    seed = EXCLUDED.seed,
                    matched_label_sample_size = EXCLUDED.matched_label_sample_size,
                    none_of_these_labels_sample_size = EXCLUDED.none_of_these_labels_sample_size,
                    created_at = EXCLUDED.created_at,
                    created_by = EXCLUDED.created_by,
                    source_filter = EXCLUDED.source_filter,
                    notes = EXCLUDED.notes
                RETURNING id
                """,
                (
                    batch["sample_batch"],
                    batch["seed"],
                    batch["matched_label_sample_size"],
                    batch["none_of_these_labels_sample_size"],
                    batch["created_at"],
                    batch["created_by"],
                    batch["source_filter"],
                    batch["notes"],
                ),
            )
            batch_id_by_slug[str(batch["sample_batch"])] = int(pg_cur.fetchone()["id"])
            imported_batches += 1

        sqlite_articles = sqlite_conn.execute(
            """
            SELECT
                sample_batch,
                target_label,
                article_id,
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
                classifier_other_phrase_used,
                target_confirmed,
                reviewer_username,
                review_notes,
                reviewed_at,
                updated_at
            FROM audit_articles
            ORDER BY sample_batch, target_label, article_id
            """
        ).fetchall()

        for row in sqlite_articles:
            batch_id = batch_id_by_slug[str(row["sample_batch"])]
            pg_cur.execute(
                """
                INSERT INTO languageingenetics.audit_sample_articles (
                    batch_id,
                    article_id,
                    target_label,
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, target_label, article_id) DO UPDATE SET
                    target_label = EXCLUDED.target_label,
                    doi = EXCLUDED.doi,
                    journal_name = EXCLUDED.journal_name,
                    pub_year = EXCLUDED.pub_year,
                    title = EXCLUDED.title,
                    abstract = EXCLUDED.abstract,
                    classifier_caucasian = EXCLUDED.classifier_caucasian,
                    classifier_white = EXCLUDED.classifier_white,
                    classifier_european = EXCLUDED.classifier_european,
                    classifier_other = EXCLUDED.classifier_other,
                    classifier_european_phrase_used = EXCLUDED.classifier_european_phrase_used,
                    classifier_other_phrase_used = EXCLUDED.classifier_other_phrase_used
                RETURNING id
                """,
                (
                    batch_id,
                    row["article_id"],
                    row["target_label"],
                    row["doi"],
                    row["journal_name"],
                    row["pub_year"],
                    row["title"],
                    row["abstract"],
                    bool(row["classifier_caucasian"]),
                    bool(row["classifier_white"]),
                    bool(row["classifier_european"]),
                    bool(row["classifier_other"]),
                    row["classifier_european_phrase_used"],
                    row["classifier_other_phrase_used"],
                ),
            )
            sample_article_id = int(pg_cur.fetchone()["id"])

            if row["target_confirmed"] is None:
                pg_cur.execute(
                    """
                    DELETE FROM languageingenetics.audit_article_reviews
                    WHERE sample_article_id = %s
                    """,
                    (sample_article_id,),
                )
                if pg_cur.rowcount:
                    cleared_reviews += 1
                continue

            pg_cur.execute(
                """
                INSERT INTO languageingenetics.audit_article_reviews (
                    sample_article_id,
                    target_confirmed,
                    reviewer_username,
                    review_notes,
                    reviewed_at,
                    updated_at,
                    source
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'merah_audit_sqlite')
                ON CONFLICT (sample_article_id) DO UPDATE SET
                    target_confirmed = EXCLUDED.target_confirmed,
                    reviewer_username = EXCLUDED.reviewer_username,
                    review_notes = EXCLUDED.review_notes,
                    reviewed_at = EXCLUDED.reviewed_at,
                    updated_at = EXCLUDED.updated_at,
                    source = EXCLUDED.source
                """,
                (
                    sample_article_id,
                    bool(row["target_confirmed"]),
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
        f"Imported {imported_batches} batches, upserted {imported_reviews} reviews, "
        f"cleared {cleared_reviews} PostgreSQL reviews from {sqlite_path}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
