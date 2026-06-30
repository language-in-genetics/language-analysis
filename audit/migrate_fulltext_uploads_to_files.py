#!/usr/bin/env python3
"""Move full-text upload blobs out of the LIG audit SQLite database."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import sqlite3
import string
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


SAFE_CHARS = set(string.ascii_letters + string.digits + "._-")


def safe_component(value: str | None, fallback: str) -> str:
    value = (value or "").strip()
    if not value:
        value = fallback
    value = Path(value).name
    cleaned = "".join(ch if ch in SAFE_CHARS else "_" for ch in value).strip("._-")
    if not cleaned:
        cleaned = fallback
    return cleaned[:120]


def default_upload_dir(sqlite_path: Path) -> Path:
    return sqlite_path.parent / "fulltext_uploads"


def ensure_schema(conn: sqlite3.Connection) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(fulltext_articles)")}
    if "uploaded_file_path" not in columns:
        conn.execute("ALTER TABLE fulltext_articles ADD COLUMN uploaded_file_path TEXT")
        conn.commit()


def backup_database(sqlite_path: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    backup_path = sqlite_path.with_name(f"{sqlite_path.stem}.pre-file-migration-{timestamp}{sqlite_path.suffix}")
    shutil.copy2(sqlite_path, backup_path)
    return backup_path


def upload_rel_path(batch: str, article_id: int, filename: str | None, data: bytes) -> PurePosixPath:
    digest = hashlib.sha256(data).hexdigest()[:16]
    clean_batch = safe_component(batch, "unknown-batch")
    clean_filename = safe_component(filename, "upload.bin")
    return PurePosixPath(clean_batch) / str(article_id) / f"{digest}_{clean_filename}"


def existing_file_matches(upload_dir: Path, rel_path: str | None, expected_size: int) -> bool:
    if not rel_path:
        return False
    rel = PurePosixPath(rel_path)
    if rel.is_absolute() or ".." in rel.parts:
        return False
    path = upload_dir / Path(*rel.parts)
    return path.is_file() and path.stat().st_size == expected_size


def write_upload_file(upload_dir: Path, rel_path: PurePosixPath, data: bytes) -> Path:
    path = upload_dir / Path(*rel_path.parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size == len(data):
        return path
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(data)
    tmp_path.chmod(0o644)
    tmp_path.replace(path)
    return path


def migrate(sqlite_path: Path, upload_dir: Path, clear_blobs: bool, dry_run: bool) -> tuple[int, int]:
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    rows = conn.execute(
        """
        SELECT
            batch_slug,
            article_id,
            uploaded_filename,
            uploaded_size,
            uploaded_file_path,
            uploaded_blob
        FROM fulltext_articles
        WHERE uploaded_blob IS NOT NULL
          AND length(uploaded_blob) > 0
        ORDER BY batch_slug, article_id
        """
    ).fetchall()

    written = 0
    cleared = 0
    for row in rows:
        data = bytes(row["uploaded_blob"])
        expected_size = len(data)
        rel_path = row["uploaded_file_path"]
        if not existing_file_matches(upload_dir, rel_path, expected_size):
            rel = upload_rel_path(row["batch_slug"], int(row["article_id"]), row["uploaded_filename"], data)
            rel_path = rel.as_posix()
            if not dry_run:
                write_upload_file(upload_dir, rel, data)
            written += 1

        if dry_run:
            continue

        if clear_blobs:
            conn.execute(
                """
                UPDATE fulltext_articles
                SET uploaded_file_path = ?,
                    uploaded_blob = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE batch_slug = ? AND article_id = ?
                """,
                (rel_path, row["batch_slug"], row["article_id"]),
            )
            cleared += 1
        else:
            conn.execute(
                """
                UPDATE fulltext_articles
                SET uploaded_file_path = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE batch_slug = ? AND article_id = ?
                """,
                (rel_path, row["batch_slug"], row["article_id"]),
            )

    if dry_run:
        conn.close()
        return written, cleared

    conn.commit()
    conn.close()
    return written, cleared


def main() -> int:
    parser = argparse.ArgumentParser(description="Move full-text upload blobs out of SQLite into a protected file directory")
    parser.add_argument("--sqlite-db", default="review_data/lig_audit.db", help="SQLite audit database path")
    parser.add_argument("--upload-dir", help="Destination upload directory; defaults next to the SQLite DB")
    parser.add_argument("--clear-blobs", action="store_true", help="Clear uploaded_blob after files have been written")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM after clearing blobs")
    parser.add_argument("--no-backup", action="store_true", help="Do not create a local database backup before changing rows")
    parser.add_argument("--dry-run", action="store_true", help="Report work without writing files or changing SQLite")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    sqlite_path_arg = Path(args.sqlite_db)
    if not sqlite_path_arg.is_absolute():
        sqlite_path_arg = script_dir / sqlite_path_arg
    sqlite_path = sqlite_path_arg.resolve()
    upload_dir = Path(args.upload_dir).resolve() if args.upload_dir else default_upload_dir(sqlite_path).resolve()
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite database not found: {sqlite_path}")

    if not args.dry_run and not args.no_backup:
        backup_path = backup_database(sqlite_path)
        print(f"Created backup: {backup_path}")

    written, cleared = migrate(sqlite_path, upload_dir, args.clear_blobs, args.dry_run)
    print(f"Upload files written or verified: {written}")
    print(f"SQLite blob rows cleared: {cleared}")

    if args.vacuum:
        if not args.clear_blobs:
            raise SystemExit("--vacuum requires --clear-blobs")
        if not args.dry_run:
            conn = sqlite3.connect(sqlite_path)
            conn.execute("VACUUM")
            conn.close()
            print("Vacuumed SQLite database")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
