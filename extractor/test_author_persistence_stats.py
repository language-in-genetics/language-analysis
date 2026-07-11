import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from author_persistence_stats import (
    build_author_persistence_statistics,
    load_author_source_rows_from_sqlite,
    normalize_author_identity,
)


def row(file_id, year, author, *, caucasian=False, white=False, european=False, other=False):
    return {
        "file_id": file_id,
        "work_id": file_id,
        "work_version_id": file_id,
        "pub_year": year,
        "caucasian": caucasian,
        "white": white,
        "european": european,
        "other": other,
        "journal_name": "Human Genetics",
        "title": f"Article {file_id}",
        "raw_json_text": json.dumps({"author": [author]}),
    }


class AuthorPersistenceStatsTests(unittest.TestCase):
    def test_normalizes_orcid_author_identity(self):
        identity = normalize_author_identity({
            "ORCID": "https://orcid.org/0000-0002-1825-0097",
            "given": "Jane",
            "family": "Smith",
        })

        self.assertEqual(identity[0], "orcid:0000-0002-1825-0097")
        self.assertEqual(identity[2], "orcid")

    def test_uses_strict_name_identity_for_full_given_name(self):
        identity = normalize_author_identity({"given": "Jane A.", "family": "Smith"})

        self.assertEqual(identity[0], "name:smith|jane a")
        self.assertEqual(identity[2], "strict_name")

    def test_skips_initial_only_name_without_orcid(self):
        self.assertIsNone(normalize_author_identity({"given": "J.", "family": "Smith"}))

    def test_builds_temporal_pair_and_recent_cohort_counts(self):
        alice = {"given": "Alice", "family": "Smith"}
        bob = {"given": "Robert", "family": "Jones"}
        carol = {"given": "Carol", "family": "Nguyen"}
        rows = [
            row(1, 2000, alice, caucasian=True),
            row(2, 2005, alice, caucasian=True),
            row(3, 2018, alice, white=True, european=True),
            row(4, 2000, bob),
            row(5, 2018, bob),
            row(6, 2000, carol),
            row(7, 2018, carol, caucasian=True),
        ]

        stats = build_author_persistence_statistics(rows, recent_start_year=2015)

        self.assertEqual(stats["population"]["analyzed_articles"], 7)
        self.assertEqual(stats["population"]["author_identities"], 3)
        self.assertEqual(stats["population"]["author_identities_with_any_caucasian"], 2)

        all_pairs = stats["all_temporal_pairs"]["test"]
        self.assertEqual(all_pairs["row1_positive"], 1)
        self.assertEqual(all_pairs["row1_negative"], 2)
        self.assertEqual(all_pairs["row2_positive"], 1)
        self.assertEqual(all_pairs["row2_negative"], 1)

        next_pairs = stats["next_paper_transitions"]["test"]
        self.assertEqual(next_pairs["row1_positive"], 1)
        self.assertEqual(next_pairs["row1_negative"], 1)
        self.assertEqual(next_pairs["row2_positive"], 1)
        self.assertEqual(next_pairs["row2_negative"], 1)

        behavior = stats["cohort"]["active_pre_caucasian_recent_behavior"]
        self.assertEqual(behavior["authors"], 1)
        self.assertEqual(behavior["recent_caucasian_authors"], 0)
        self.assertEqual(behavior["recent_white_or_european_without_caucasian_authors"], 1)

        json.dumps(stats)

    def test_loads_author_rows_from_sqlite_by_doi(self):
        processed_rows = [
            {
                "file_id": 1,
                "work_id": 1,
                "work_version_id": 1,
                "pub_year": 2020,
                "caucasian": True,
                "white": False,
                "european": False,
                "other": False,
                "journal_name": "Human Genetics",
                "title": "Article 1",
                "normalized_doi": "10.1000/example",
            }
        ]

        class FakeCursor:
            def execute(self, _sql):
                return None

            def fetchall(self):
                return processed_rows

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "focused.sqlite"
            conn = sqlite3.connect(path)
            conn.execute("CREATE TABLE import_records (raw_json_text TEXT NOT NULL)")
            conn.execute(
                "INSERT INTO import_records (raw_json_text) VALUES (?)",
                (json.dumps({
                    "DOI": "10.1000/example",
                    "author": [{"given": "Alice", "family": "Smith"}],
                }),),
            )
            conn.commit()
            conn.close()

            rows = load_author_source_rows_from_sqlite(FakeCursor(), str(path))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["author_given"], "Alice")
        self.assertEqual(rows[0]["author_family"], "Smith")


if __name__ == "__main__":
    unittest.main()
