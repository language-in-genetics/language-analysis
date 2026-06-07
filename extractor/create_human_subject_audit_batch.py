#!/usr/bin/env python3
"""
Create a reproducible human audit batch for the Homo sapiens title/abstract classifier.

This seeds:
1. PostgreSQL under languageingenetics.human_subject_audit_*
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


@dataclass(frozen=True)
class HumanSubjectBatchConfig:
    slug: str
    seed: int
    sample_size: int
    ai_human_size: int
    ai_not_human_size: int
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


def resolve_sample_sizes(args: argparse.Namespace) -> tuple[int, int]:
    if args.human_size is None and args.not_human_size is None:
        human_size = args.sample_size // 2
        not_human_size = args.sample_size - human_size
        return human_size, not_human_size
    human_size = args.human_size if args.human_size is not None else args.sample_size - args.not_human_size
    not_human_size = args.not_human_size if args.not_human_size is not None else args.sample_size - args.human_size
    if human_size < 0 or not_human_size < 0:
        raise ValueError("Sample sizes must be non-negative")
    if human_size + not_human_size != args.sample_size:
        raise ValueError(
            f"--human-size plus --not-human-size must equal --sample-size ({human_size} + {not_human_size} != {args.sample_size})"
        )
    return human_size, not_human_size


def build_filters(
    args: argparse.Namespace,
) -> tuple[list[str], list[object], list[str], list[object]]:
    h_clauses = [
        "h.processed = true",
        "h.about_humans IS NOT NULL",
    ]
    h_params: list[object] = []

    cw_clauses = [
        "cw.title IS NOT NULL",
        "j.enabled = true",
    ]
    cw_params: list[object] = []

    if args.min_year is not None:
        cw_clauses.append("cw.pub_year >= %s")
        cw_params.append(args.min_year)
    if args.max_year is not None:
        cw_clauses.append("cw.pub_year <= %s")
        cw_params.append(args.max_year)
    if args.journal:
        cw_clauses.append("cw.journal_name = %s")
        cw_params.append(args.journal)
    if args.require_abstract:
        h_clauses.append("h.has_abstract = true")
    if args.confidence:
        h_clauses.append("h.confidence = %s")
        h_params.append(args.confidence)
    if args.exclude_existing:
        h_clauses.append(
            """
            NOT EXISTS (
                SELECT 1
                FROM languageingenetics.human_subject_audit_articles existing
                WHERE existing.human_subject_classification_id = h.id
            )
            """
        )

    return h_clauses, h_params, cw_clauses, cw_params


def fetch_candidate_ids(
    cur,
    about_humans: bool,
    h_clauses: list[str],
    h_params: list[object],
    cw_clauses: list[str],
    cw_params: list[object],
) -> list[int]:
    h_filter = h_clauses + ["h.about_humans = %s"]
    cur.execute(
        f"""
        WITH h_filtered AS MATERIALIZED (
            SELECT
                h.id,
                h.work_version_id
            FROM languageingenetics.human_subject_classifications h
            WHERE {' AND '.join(h_filter)}
        )
        SELECT h.id
        FROM h_filtered h
        JOIN public.crossref_current_works cw
          ON cw.work_version_id = h.work_version_id
        JOIN languageingenetics.journals j
          ON j.name = cw.journal_name
        WHERE {' AND '.join(cw_clauses)}
        ORDER BY h.id
        """,
        h_params + [about_humans] + cw_params,
    )
    return [int(row["id"]) for row in cur.fetchall()]


def choose_ids(candidate_ids: list[int], sample_size: int, seed: int) -> list[int]:
    if sample_size == 0:
        return []
    if len(candidate_ids) < sample_size:
        raise ValueError(
            f"Requested sample size {sample_size}, but only found {len(candidate_ids)} candidates."
        )
    rng = random.Random(seed)
    return sorted(rng.sample(candidate_ids, sample_size))


def fetch_details(cur, classification_ids: list[int]) -> list[dict]:
    if not classification_ids:
        return []
    cur.execute(
        """
        WITH selected_classifications AS MATERIALIZED (
            SELECT
                h.id AS classification_id,
                h.article_id,
                h.work_id,
                h.work_version_id,
                h.about_humans,
                h.human_evidence,
                h.confidence,
                h.model,
                h.prompt_tokens,
                h.completion_tokens
            FROM languageingenetics.human_subject_classifications h
            WHERE h.id = ANY(%s)
        )
        SELECT
            h.classification_id AS classification_id,
            h.article_id,
            h.work_id,
            h.work_version_id,
            COALESCE(cw.original_doi, cw.normalized_doi, '') AS doi,
            cw.journal_name,
            cw.pub_year,
            cw.title,
            cw.abstract,
            h.about_humans,
            h.human_evidence,
            h.confidence,
            h.model,
            h.prompt_tokens,
            h.completion_tokens
        FROM selected_classifications h
        JOIN public.crossref_current_works cw
          ON cw.work_version_id = h.work_version_id
        """,
        (classification_ids,),
    )
    by_id = {int(row["classification_id"]): row for row in cur.fetchall()}
    missing = [classification_id for classification_id in classification_ids if classification_id not in by_id]
    if missing:
        raise RuntimeError(f"Failed to load details for classification IDs: {missing[:10]}")
    return [by_id[classification_id] for classification_id in classification_ids]


def batch_exists_pg(cur, slug: str) -> bool:
    cur.execute(
        "SELECT 1 FROM languageingenetics.human_subject_audit_batches WHERE slug = %s LIMIT 1",
        (slug,),
    )
    return cur.fetchone() is not None


def batch_exists_sqlite(conn: sqlite3.Connection, slug: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM human_subject_audit_batches WHERE batch_slug = ?",
        (slug,),
    ).fetchone()
    return row is not None


def insert_pg_batch(cur, cfg: HumanSubjectBatchConfig) -> int:
    cur.execute(
        """
        INSERT INTO languageingenetics.human_subject_audit_batches (
            slug,
            seed,
            sample_size,
            ai_human_sample_size,
            ai_not_human_sample_size,
            created_at,
            created_by,
            source_filter,
            notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            cfg.slug,
            cfg.seed,
            cfg.sample_size,
            cfg.ai_human_size,
            cfg.ai_not_human_size,
            cfg.created_at,
            cfg.created_by,
            cfg.source_filter,
            cfg.notes,
        ),
    )
    return int(cur.fetchone()["id"])


def insert_pg_articles(cur, batch_id: int, rows: list[dict]) -> None:
    if not rows:
        return
    psycopg2.extras.execute_batch(
        cur,
        """
        INSERT INTO languageingenetics.human_subject_audit_articles (
            batch_id,
            human_subject_classification_id,
            article_id,
            work_id,
            work_version_id,
            doi,
            journal_name,
            pub_year,
            title,
            abstract,
            ai_about_humans,
            ai_evidence,
            ai_confidence,
            ai_model,
            ai_prompt_tokens,
            ai_completion_tokens
        )
        VALUES (
            %(batch_id)s,
            %(classification_id)s,
            %(article_id)s,
            %(work_id)s,
            %(work_version_id)s,
            %(doi)s,
            %(journal_name)s,
            %(pub_year)s,
            %(title)s,
            %(abstract)s,
            %(about_humans)s,
            %(human_evidence)s,
            %(confidence)s,
            %(model)s,
            %(prompt_tokens)s,
            %(completion_tokens)s
        )
        """,
        [
            {
                "batch_id": batch_id,
                "classification_id": int(row["classification_id"]),
                "article_id": row["article_id"],
                "work_id": row["work_id"],
                "work_version_id": row["work_version_id"],
                "doi": row["doi"],
                "journal_name": row["journal_name"],
                "pub_year": row["pub_year"],
                "title": row["title"],
                "abstract": row["abstract"],
                "about_humans": bool(row["about_humans"]),
                "human_evidence": row["human_evidence"],
                "confidence": row["confidence"],
                "model": row["model"],
                "prompt_tokens": row["prompt_tokens"],
                "completion_tokens": row["completion_tokens"],
            }
            for row in rows
        ],
        page_size=200,
    )


def insert_sqlite_batch(conn: sqlite3.Connection, cfg: HumanSubjectBatchConfig) -> None:
    conn.execute(
        """
        INSERT INTO human_subject_audit_batches (
            batch_slug,
            seed,
            sample_size,
            ai_human_sample_size,
            ai_not_human_sample_size,
            created_at,
            created_by,
            source_filter,
            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cfg.slug,
            cfg.seed,
            cfg.sample_size,
            cfg.ai_human_size,
            cfg.ai_not_human_size,
            cfg.created_at,
            cfg.created_by,
            cfg.source_filter,
            cfg.notes,
        ),
    )


def insert_sqlite_articles(conn: sqlite3.Connection, cfg: HumanSubjectBatchConfig, rows: list[dict]) -> None:
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO human_subject_audit_articles (
            batch_slug,
            classification_id,
            article_id,
            work_id,
            work_version_id,
            doi,
            journal_name,
            pub_year,
            title,
            abstract,
            ai_about_humans,
            ai_evidence,
            ai_confidence,
            ai_model,
            ai_prompt_tokens,
            ai_completion_tokens
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                cfg.slug,
                int(row["classification_id"]),
                row["article_id"],
                row["work_id"],
                row["work_version_id"],
                row["doi"],
                row["journal_name"],
                row["pub_year"],
                row["title"],
                row["abstract"],
                1 if row["about_humans"] else 0,
                row["human_evidence"],
                row["confidence"],
                row["model"],
                row["prompt_tokens"],
                row["completion_tokens"],
            )
            for row in rows
        ],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a reproducible Homo sapiens human-audit batch")
    parser.add_argument("--sqlite-db", default="../audit/review_data/lig_audit.db", help="SQLite database path to seed/update")
    parser.add_argument("--batch-slug", help="Batch slug to create (default: human-subject-YYYYMMDD-seedN)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--sample-size", type=int, default=200, help="Total sample size")
    parser.add_argument("--human-size", type=int, help="AI-about-humans sample size")
    parser.add_argument("--not-human-size", type=int, help="AI-not-about-humans sample size")
    parser.add_argument("--min-year", type=int, help="Minimum publication year")
    parser.add_argument("--max-year", type=int, help="Maximum publication year")
    parser.add_argument("--journal", help="Restrict sampling to one journal")
    parser.add_argument("--require-abstract", action="store_true", help="Only sample articles with abstracts")
    parser.add_argument("--confidence", choices=["high", "medium", "low"], help="Restrict to one AI confidence value")
    parser.add_argument("--exclude-existing", action="store_true", help="Exclude articles already sampled in any Homo sapiens audit batch")
    parser.add_argument("--created-by", default=os.environ.get("USER", "unknown"), help="Batch creator label")
    parser.add_argument("--notes", help="Optional batch notes")
    args = parser.parse_args()

    human_size, not_human_size = resolve_sample_sizes(args)
    now = datetime.now(timezone.utc)
    slug = args.batch_slug or f"human-subject-{now.strftime('%Y%m%d')}-seed{args.seed}"
    source_parts = [
        "human_subject_classifications.processed=true",
        "scope=crossref_current_works_enabled_journals",
        f"ai_human_size={human_size}",
        f"ai_not_human_size={not_human_size}",
    ]
    if args.min_year is not None:
        source_parts.append(f"min_year={args.min_year}")
    if args.max_year is not None:
        source_parts.append(f"max_year={args.max_year}")
    if args.journal:
        source_parts.append(f"journal={args.journal}")
    if args.require_abstract:
        source_parts.append("require_abstract=true")
    if args.confidence:
        source_parts.append(f"confidence={args.confidence}")
    if args.exclude_existing:
        source_parts.append("exclude_existing=true")

    cfg = HumanSubjectBatchConfig(
        slug=slug,
        seed=args.seed,
        sample_size=args.sample_size,
        ai_human_size=human_size,
        ai_not_human_size=not_human_size,
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

        h_clauses, h_params, cw_clauses, cw_params = build_filters(args)
        human_candidates = fetch_candidate_ids(
            pg_cur, True, h_clauses, h_params, cw_clauses, cw_params
        )
        not_human_candidates = fetch_candidate_ids(
            pg_cur, False, h_clauses, h_params, cw_clauses, cw_params
        )
        human_ids = choose_ids(human_candidates, human_size, args.seed)
        not_human_ids = choose_ids(not_human_candidates, not_human_size, args.seed + 1)
        rows = fetch_details(pg_cur, human_ids + not_human_ids)

        batch_id = insert_pg_batch(pg_cur, cfg)
        insert_pg_articles(pg_cur, batch_id, rows)
        insert_sqlite_batch(sqlite_conn, cfg)
        insert_sqlite_articles(sqlite_conn, cfg, rows)

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
        f"Created Homo sapiens audit batch {cfg.slug}: "
        f"ai_about_humans={human_size}, ai_not_about_humans={not_human_size}."
    )
    print(f"SQLite DB: {sqlite_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
