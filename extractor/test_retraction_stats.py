import json
import sqlite3
import tempfile
import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from retraction_stats import (
    build_retraction_statistics_from_work_ids,
    build_retraction_statistics,
    chi_square_test_2x2,
    classify_retraction_status,
    fisher_exact_two_sided,
    load_retraction_status_from_sqlite,
)


class RetractionStatsTests(unittest.TestCase):
    def test_classifies_retracted_article_title(self):
        row = {
            "normalized_doi": "10.1000/example",
            "title": "RETRACTED ARTICLE: Example genetics paper",
            "raw_json_text": "{}",
        }

        status = classify_retraction_status(row)

        self.assertTrue(status.is_retracted_article)
        self.assertFalse(status.is_retraction_notice)

    def test_classifies_retraction_notice_as_excluded_notice(self):
        row = {
            "normalized_doi": "10.1000/notice",
            "title": "Retraction Note to: Example genetics paper",
            "raw_json_text": '{"update-to":[{"DOI":"10.1000/original","type":"retraction"}]}',
        }

        status = classify_retraction_status(row)

        self.assertFalse(status.is_retracted_article)
        self.assertTrue(status.is_retraction_notice)

    def test_ignores_false_positive_retraction_word(self):
        row = {
            "normalized_doi": "10.1000/duane",
            "title": "Duane retraction syndrome in a family",
            "raw_json_text": "{}",
        }

        status = classify_retraction_status(row)

        self.assertFalse(status.is_retracted_article)
        self.assertFalse(status.is_retraction_notice)

    def test_retracted_article_with_expression_update_stays_case(self):
        rows = [
            {
                "normalized_doi": "10.1000/retracted",
                "journal_name": "Human Genetics",
                "pub_year": 2020,
                "title": "RETRACTED ARTICLE: Example genetics paper",
                "raw_json_text": (
                    '{"update-to":['
                    '{"DOI":"10.1000/retracted","type":"expression_of_concern"}'
                    ']}'
                ),
                "caucasian": False,
                "white": True,
                "european": False,
                "other": False,
            },
            {
                "normalized_doi": "10.1000/control",
                "journal_name": "Human Genetics",
                "pub_year": 2020,
                "title": "Example genetics paper",
                "raw_json_text": "{}",
                "caucasian": False,
                "white": False,
                "european": False,
                "other": False,
            },
        ]

        stats = build_retraction_statistics(rows)

        self.assertEqual(stats["population"]["eligible_articles"], 2)
        self.assertEqual(stats["population"]["retracted_articles"], 1)
        self.assertEqual(stats["population"]["excluded_expression_of_concern"], 0)

    def test_fisher_exact_detects_strong_2x2_difference(self):
        p_value = fisher_exact_two_sided(1, 9, 11, 3)

        self.assertIsNotNone(p_value)
        self.assertLess(p_value, 0.01)

    def test_chi_square_p_value_for_no_difference(self):
        statistic, p_value = chi_square_test_2x2(5, 5, 50, 50)

        self.assertEqual(statistic, 0)
        self.assertEqual(p_value, 1)

    def test_build_stats_excludes_retraction_notices(self):
        rows = [
            {
                "normalized_doi": "10.1000/retracted",
                "journal_name": "Human Genetics",
                "pub_year": 2020,
                "title": "Retracted: Example genetics paper",
                "raw_json_text": "{}",
                "caucasian": True,
                "white": False,
                "european": False,
                "other": False,
            },
            {
                "normalized_doi": "10.1000/control",
                "journal_name": "Human Genetics",
                "pub_year": 2020,
                "title": "Example genetics paper",
                "raw_json_text": "{}",
                "caucasian": False,
                "white": False,
                "european": False,
                "other": False,
            },
            {
                "normalized_doi": "10.1000/notice",
                "journal_name": "Human Genetics",
                "pub_year": 2020,
                "title": "Retraction Notice to: Example genetics paper",
                "raw_json_text": '{"update-to":[{"DOI":"10.1000/retracted","type":"retraction"}]}',
                "caucasian": True,
                "white": True,
                "european": False,
                "other": False,
            },
        ]

        stats = build_retraction_statistics(rows)

        self.assertEqual(stats["population"]["eligible_articles"], 2)
        self.assertEqual(stats["population"]["retracted_articles"], 1)
        self.assertEqual(stats["population"]["excluded_retraction_notices"], 1)
        any_test = next(test for test in stats["tests"] if test["outcome"] == "any_race_language")
        self.assertEqual(any_test["retracted_with_term"], 1)
        self.assertEqual(any_test["non_retracted_with_term"], 0)

    def test_build_stats_from_precomputed_work_ids(self):
        rows = [
            {"work_id": 1, "work_version_id": 10, "caucasian": True, "white": False, "european": False, "other": False},
            {"work_id": 2, "work_version_id": 20, "caucasian": False, "white": False, "european": False, "other": False},
            {"work_id": 3, "work_version_id": 30, "caucasian": True, "white": True, "european": False, "other": False},
        ]
        status = {
            "retracted_work_ids": {1},
            "retraction_notice_work_ids": {3},
            "expression_notice_work_ids": set(),
            "examples_by_work_id": {
                1: {"doi": "10.1000/retracted", "title": "Retracted: Example"}
            },
            "source": "focused.jsonl.gz",
        }

        stats = build_retraction_statistics_from_work_ids(rows, status)

        self.assertEqual(stats["population"]["eligible_articles"], 2)
        self.assertEqual(stats["population"]["retracted_articles"], 1)
        self.assertEqual(stats["population"]["excluded_retraction_notices"], 1)
        self.assertEqual(stats["retracted_examples"][0]["work_version_id"], 10)

    def test_load_retraction_status_from_sqlite_stage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "focused.sqlite"
            conn = sqlite3.connect(path)
            conn.execute(
                """
                CREATE TABLE import_records (
                    id INTEGER PRIMARY KEY,
                    category TEXT NOT NULL,
                    source_ref TEXT NOT NULL,
                    raw_json_text TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO import_records (category, source_ref, raw_json_text) VALUES (?, ?, ?)",
                (
                    "focused",
                    "0.jsonl.gz:1",
                    json.dumps({
                        "DOI": "10.1000/retracted",
                        "title": ["RETRACTED ARTICLE: Example genetics paper"],
                        "container-title": ["Human Genetics"],
                    }),
                ),
            )
            conn.commit()
            conn.close()

            status = load_retraction_status_from_sqlite(str(path))

        self.assertEqual(status["records_scanned"], 1)
        self.assertEqual(status["retracted_dois"], {"10.1000/retracted"})


if __name__ == "__main__":
    unittest.main()
