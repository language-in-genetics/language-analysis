"""Shared-author terminology persistence statistics for the LIG pipeline."""

from __future__ import annotations

import csv
import html
import json
import math
import re
import sqlite3
import unicodedata
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from retraction_stats import chi_square_test_2x2, fisher_exact_two_sided


DEFAULT_RECENT_START_YEAR = 2015
DEFAULT_PEAK_YEAR = 2006

PROCESSED_ANALYSIS_SQL = """
    SELECT
        f.id AS file_id,
        f.work_id,
        f.work_version_id,
        COALESCE(f.pub_year, v.pub_year) AS pub_year,
        COALESCE(f.caucasian, false) AS caucasian,
        COALESCE(f.white, false) AS white,
        COALESCE(f.european, false) AS european,
        COALESCE(f.other, false) AS other,
        v.journal_name,
        v.title,
        w.normalized_doi
    FROM languageingenetics.files f
    JOIN public.crossref_work_versions v
      ON v.id = f.work_version_id
    LEFT JOIN public.crossref_works w
      ON w.id = f.work_id
    JOIN languageingenetics.journals j
      ON j.name = v.journal_name
     AND j.enabled = true
    WHERE f.processed = true
      AND f.work_version_id IS NOT NULL
      AND COALESCE(f.pub_year, v.pub_year) IS NOT NULL
    ORDER BY COALESCE(f.pub_year, v.pub_year), f.work_version_id, f.id
"""

AUTHOR_SOURCE_SQL = """
    SELECT
        f.id AS file_id,
        f.work_id,
        f.work_version_id,
        COALESCE(f.pub_year, v.pub_year) AS pub_year,
        COALESCE(f.caucasian, false) AS caucasian,
        COALESCE(f.white, false) AS white,
        COALESCE(f.european, false) AS european,
        COALESCE(f.other, false) AS other,
        v.journal_name,
        v.title,
        author_item.ordinality AS author_position,
        author_item.author ->> 'ORCID' AS author_orcid,
        author_item.author ->> 'given' AS author_given,
        author_item.author ->> 'family' AS author_family,
        author_item.author ->> 'name' AS author_name
    FROM languageingenetics.files f
    JOIN public.crossref_work_versions v
      ON v.id = f.work_version_id
    JOIN languageingenetics.journals j
      ON j.name = v.journal_name
     AND j.enabled = true
    CROSS JOIN LATERAL (
        SELECT v.raw_json_text::jsonb AS raw_json
    ) raw
    LEFT JOIN LATERAL jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(raw.raw_json -> 'author') = 'array'
                THEN raw.raw_json -> 'author'
            ELSE '[]'::jsonb
        END
    ) WITH ORDINALITY AS author_item(author, ordinality)
      ON true
    WHERE f.processed = true
      AND f.work_version_id IS NOT NULL
      AND COALESCE(f.pub_year, v.pub_year) IS NOT NULL
    ORDER BY COALESCE(f.pub_year, v.pub_year), f.work_version_id, f.id, author_item.ordinality
"""

TERMS = ("caucasian", "white", "european", "other")


@dataclass(frozen=True)
class ArticleRecord:
    file_id: int
    work_id: int | None
    work_version_id: int | None
    pub_year: int
    caucasian: bool
    white: bool
    european: bool
    other: bool
    journal_name: str | None = None
    title: str | None = None

    @property
    def any_term(self) -> bool:
        return self.caucasian or self.white or self.european or self.other

    @property
    def white_or_european(self) -> bool:
        return self.white or self.european

    @property
    def white_or_european_without_caucasian(self) -> bool:
        return (self.white or self.european) and not self.caucasian


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        value = " ".join(str(item) for item in value if item is not None)
    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _ascii_fold(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _normalize_name_part(value: Any) -> str:
    text = _ascii_fold(str(value or "")).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_orcid(value: Any) -> str | None:
    """Return a bare ORCID identifier when Crossref supplies one."""
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dXx])", text)
    if not match:
        return None
    return match.group(1).upper()


def normalize_doi(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_author_identity(author: Mapping[str, Any]) -> tuple[str, str, str] | None:
    """Return (identity_key, display_name, match_method) for a Crossref author.

    ORCID is preferred. Otherwise, use a conservative name key requiring family
    name plus a non-initial-only given name. This avoids many false matches such
    as "J. Smith" while still allowing stable full-name matches.
    """
    orcid = normalize_orcid(author.get("ORCID") or author.get("orcid"))
    given_display = _clean_text(author.get("given"))
    family_display = _clean_text(author.get("family"))
    name_display = _clean_text(author.get("name"))
    display = " ".join(part for part in (given_display, family_display) if part).strip()
    display = display or name_display or orcid or ""

    if orcid:
        return f"orcid:{orcid}", display, "orcid"

    family = _normalize_name_part(author.get("family"))
    given = _normalize_name_part(author.get("given"))
    if not family or not given:
        return None
    given_tokens = given.split()
    if all(len(token) == 1 for token in given_tokens):
        return None
    if len(family) < 2:
        return None
    return f"name:{family}|{given}", display, "strict_name"


def _parse_raw_json(raw_json_text: Any) -> dict[str, Any]:
    if isinstance(raw_json_text, Mapping):
        return dict(raw_json_text)
    if not raw_json_text:
        return {}
    try:
        parsed = json.loads(raw_json_text)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _authors_from_raw_json(raw_json_text: Any) -> list[Mapping[str, Any]]:
    raw = _parse_raw_json(raw_json_text)
    authors = raw.get("author") or []
    if isinstance(authors, Mapping):
        authors = [authors]
    if not isinstance(authors, list):
        return []
    return [author for author in authors if isinstance(author, Mapping)]


def _raw_json_doi(raw_json_text: Any) -> str:
    raw = _parse_raw_json(raw_json_text)
    return normalize_doi(raw.get("DOI") or raw.get("doi"))


def _raw_author_rows_for_processed_row(
    processed_row: Mapping[str, Any],
    raw_authors: list[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not raw_authors:
        row = dict(processed_row)
        row.update({
            "author_position": None,
            "author_orcid": None,
            "author_given": None,
            "author_family": None,
            "author_name": None,
        })
        return [row]

    rows = []
    for index, author in enumerate(raw_authors, start=1):
        row = dict(processed_row)
        row.update({
            "author_position": index,
            "author_orcid": author.get("ORCID") or author.get("orcid"),
            "author_given": author.get("given"),
            "author_family": author.get("family"),
            "author_name": author.get("name"),
        })
        rows.append(row)
    return rows


def load_author_source_rows_from_sqlite(cursor: Any, sqlite_path: str) -> list[dict[str, Any]]:
    """Build compact author rows from a focused Crossref SQLite stage.

    PostgreSQL is still used for the processed analysis rows, but authors are
    read from the local SQLite stage by DOI. This avoids scanning the large
    PostgreSQL raw_json_text payload during dashboard generation.
    """
    cursor.execute(PROCESSED_ANALYSIS_SQL)
    processed_rows = [dict(row) for row in cursor.fetchall()]
    rows_by_doi: dict[str, list[dict[str, Any]]] = defaultdict(list)
    output_rows: list[dict[str, Any]] = []
    matched_file_ids: set[int] = set()

    for row in processed_rows:
        doi = normalize_doi(row.get("normalized_doi"))
        if doi:
            rows_by_doi[doi].append(row)

    sqlite_conn = sqlite3.connect(sqlite_path)
    try:
        for (raw_json_text,) in sqlite_conn.execute("SELECT raw_json_text FROM import_records"):
            doi = _raw_json_doi(raw_json_text)
            if not doi:
                continue
            matching_rows = rows_by_doi.get(doi)
            if not matching_rows:
                continue
            raw_authors = _authors_from_raw_json(raw_json_text)
            for processed_row in matching_rows:
                matched_file_ids.add(int(processed_row["file_id"]))
                output_rows.extend(_raw_author_rows_for_processed_row(processed_row, raw_authors))
    finally:
        sqlite_conn.close()

    for processed_row in processed_rows:
        if int(processed_row["file_id"]) in matched_file_ids:
            continue
        output_rows.extend(_raw_author_rows_for_processed_row(processed_row, None))

    output_rows.sort(key=lambda row: (
        row.get("pub_year") or 0,
        row.get("work_version_id") or 0,
        row.get("file_id") or 0,
        row.get("author_position") or 0,
    ))
    return output_rows


def _article_from_row(row: Mapping[str, Any]) -> ArticleRecord | None:
    file_id = row.get("file_id")
    pub_year = row.get("pub_year")
    if file_id is None or pub_year is None:
        return None
    return ArticleRecord(
        file_id=int(file_id),
        work_id=int(row["work_id"]) if row.get("work_id") is not None else None,
        work_version_id=int(row["work_version_id"]) if row.get("work_version_id") is not None else None,
        pub_year=int(pub_year),
        caucasian=bool(row.get("caucasian")),
        white=bool(row.get("white")),
        european=bool(row.get("european")),
        other=bool(row.get("other")),
        journal_name=row.get("journal_name"),
        title=row.get("title"),
    )


def prepare_author_dataset(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Parse processed article rows into author identities and article maps."""
    articles: dict[int, ArticleRecord] = {}
    authors: dict[str, dict[str, Any]] = {}
    article_author_ids: dict[int, set[str]] = defaultdict(set)
    method_counts = defaultdict(int)
    raw_author_rows = 0
    matched_author_rows = 0
    skipped_author_rows = 0
    articles_with_author_json: set[int] = set()
    seen_author_ids_by_article: dict[int, set[str]] = defaultdict(set)

    for row in rows:
        article = _article_from_row(row)
        if article is None:
            continue
        articles[article.file_id] = article
        if "author_position" in row:
            if row.get("author_position") is None:
                raw_authors = []
            else:
                raw_authors = [{
                    "ORCID": row.get("author_orcid"),
                    "given": row.get("author_given"),
                    "family": row.get("author_family"),
                    "name": row.get("author_name"),
                }]
        else:
            raw_authors = _authors_from_raw_json(row.get("raw_json_text"))
        if raw_authors:
            articles_with_author_json.add(article.file_id)
        for raw_author in raw_authors:
            raw_author_rows += 1
            identity = normalize_author_identity(raw_author)
            if identity is None:
                skipped_author_rows += 1
                continue
            identity_key, display_name, match_method = identity
            seen_article_author_ids = seen_author_ids_by_article[article.file_id]
            if identity_key in seen_article_author_ids:
                continue
            seen_article_author_ids.add(identity_key)
            matched_author_rows += 1
            article_author_ids[article.file_id].add(identity_key)
            method_counts[match_method] += 1
            entry = authors.setdefault(
                identity_key,
                {
                    "identity": identity_key,
                    "display_name": display_name,
                    "match_method": match_method,
                    "articles": {},
                },
            )
            if not entry.get("display_name") and display_name:
                entry["display_name"] = display_name
            entry["articles"][article.file_id] = article

    identity_method_counts = defaultdict(int)
    for entry in authors.values():
        identity_method_counts[entry["match_method"]] += 1

    return {
        "articles": articles,
        "authors": authors,
        "article_author_ids": article_author_ids,
        "population": {
            "analyzed_articles": len(articles),
            "articles_with_author_json": len(articles_with_author_json),
            "articles_with_matched_author": len(article_author_ids),
            "author_identities": len(authors),
            "raw_author_rows": raw_author_rows,
            "matched_author_rows": matched_author_rows,
            "skipped_author_rows": skipped_author_rows,
            "author_identity_method_counts": dict(identity_method_counts),
            "matched_author_row_method_counts": dict(method_counts),
        },
    }


def _sorted_articles(author_entry: Mapping[str, Any]) -> list[ArticleRecord]:
    articles = list(author_entry.get("articles", {}).values())
    articles.sort(key=lambda article: (
        article.pub_year,
        article.work_version_id or 0,
        article.file_id,
    ))
    return articles


def _rate(part: int, whole: int) -> float | None:
    return part / whole if whole else None


def _risk_difference(a: int, b: int, c: int, d: int) -> float | None:
    rate1 = _rate(a, a + b)
    rate2 = _rate(c, c + d)
    if rate1 is None or rate2 is None:
        return None
    return rate1 - rate2


def _odds_ratio_haldane(a: int, b: int, c: int, d: int) -> float | None:
    if (a + b) == 0 or (c + d) == 0:
        return None
    return ((a + 0.5) * (d + 0.5)) / ((b + 0.5) * (c + 0.5))


def _binary_test(
    *,
    test_id: str,
    label: str,
    unit: str,
    row1_label: str,
    row2_label: str,
    positive_label: str,
    negative_label: str,
    a: int,
    b: int,
    c: int,
    d: int,
) -> dict[str, Any]:
    chi_square, chi_square_p = chi_square_test_2x2(a, b, c, d)
    return {
        "test_id": test_id,
        "label": label,
        "unit": unit,
        "row1_label": row1_label,
        "row2_label": row2_label,
        "positive_label": positive_label,
        "negative_label": negative_label,
        "row1_positive": a,
        "row1_negative": b,
        "row2_positive": c,
        "row2_negative": d,
        "row1_rate": _rate(a, a + b),
        "row2_rate": _rate(c, c + d),
        "risk_difference": _risk_difference(a, b, c, d),
        "odds_ratio_haldane": _odds_ratio_haldane(a, b, c, d),
        "fisher_exact_p": fisher_exact_two_sided(a, b, c, d),
        "chi_square": chi_square,
        "chi_square_p": chi_square_p,
    }


def _chi_square_test_table(table: list[list[int]]) -> tuple[float | None, float | None, int]:
    """Pearson chi-square test for a contingency table.

    The current report uses this for 2x3 tables, where the survival function is
    exp(-x/2). For 2x2 tables, use the exact/Fisher helpers above.
    """
    row_count = len(table)
    col_count = len(table[0]) if table else 0
    if row_count < 2 or col_count < 2:
        return None, None, 0
    row_sums = [sum(row) for row in table]
    col_sums = [sum(table[row][col] for row in range(row_count)) for col in range(col_count)]
    total = sum(row_sums)
    if total == 0 or any(value == 0 for value in row_sums) or any(value == 0 for value in col_sums):
        return None, None, (row_count - 1) * (col_count - 1)

    statistic = 0.0
    for row_index, row in enumerate(table):
        for col_index, observed in enumerate(row):
            expected = row_sums[row_index] * col_sums[col_index] / total
            statistic += ((observed - expected) ** 2) / expected

    df = (row_count - 1) * (col_count - 1)
    if df == 1:
        p_value = math.erfc(math.sqrt(statistic / 2))
    elif df == 2:
        p_value = math.exp(-statistic / 2)
    else:
        p_value = None
    return statistic, p_value, df


def _empty_term_counts() -> dict[str, int]:
    return {
        "denominator": 0,
        "caucasian": 0,
        "white": 0,
        "european": 0,
        "other": 0,
        "any_term": 0,
        "white_or_european": 0,
        "white_or_european_without_caucasian": 0,
    }


def _add_later_term_counts(term_counts: dict[str, int], article: ArticleRecord) -> None:
    term_counts["denominator"] += 1
    for term in TERMS:
        if getattr(article, term):
            term_counts[term] += 1
    if article.any_term:
        term_counts["any_term"] += 1
    if article.white_or_european:
        term_counts["white_or_european"] += 1
    if article.white_or_european_without_caucasian:
        term_counts["white_or_european_without_caucasian"] += 1


def _finalize_term_counts(term_counts: Mapping[str, int]) -> dict[str, Any]:
    denominator = int(term_counts.get("denominator", 0))
    result = dict(term_counts)
    for key, value in list(term_counts.items()):
        if key == "denominator":
            continue
        result[f"{key}_rate"] = _rate(int(value), denominator)
    return result


def build_all_temporal_pair_summary(authors: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """Count all earlier-to-later shared-author paper pairs."""
    a = b = c = d = 0
    identities_with_temporal_pairs = 0
    identities_with_prior_caucasian_pairs = 0
    later_terms_after_prior_caucasian = _empty_term_counts()

    for entry in authors.values():
        articles = _sorted_articles(entry)
        if len(articles) < 2:
            continue
        years = [article.pub_year for article in articles]
        n = len(articles)
        suffix_total = [0] * (n + 1)
        suffix_counts = {key: [0] * (n + 1) for key in (
            "caucasian",
            "white",
            "european",
            "other",
            "any_term",
            "white_or_european",
            "white_or_european_without_caucasian",
        )}
        for index in range(n - 1, -1, -1):
            article = articles[index]
            suffix_total[index] = suffix_total[index + 1] + 1
            for key, counts in suffix_counts.items():
                counts[index] = counts[index + 1] + int(bool(getattr(article, key)))

        identity_has_pair = False
        identity_has_prior_c_pair = False
        for article in articles:
            later_index = bisect_right(years, article.pub_year)
            later_total = suffix_total[later_index]
            if later_total == 0:
                continue
            identity_has_pair = True
            later_caucasian = suffix_counts["caucasian"][later_index]
            if article.caucasian:
                a += later_caucasian
                b += later_total - later_caucasian
                identity_has_prior_c_pair = True
                later_terms_after_prior_caucasian["denominator"] += later_total
                for key, counts in suffix_counts.items():
                    later_terms_after_prior_caucasian[key] += counts[later_index]
            else:
                c += later_caucasian
                d += later_total - later_caucasian

        if identity_has_pair:
            identities_with_temporal_pairs += 1
        if identity_has_prior_c_pair:
            identities_with_prior_caucasian_pairs += 1

    test = _binary_test(
        test_id="all_temporal_shared_author_pairs",
        label="Prior Caucasian use predicts later Caucasian use",
        unit="all temporal shared-author paper pairs",
        row1_label="Earlier paper used Caucasian",
        row2_label="Earlier paper did not use Caucasian",
        positive_label="Later paper used Caucasian",
        negative_label="Later paper did not use Caucasian",
        a=a,
        b=b,
        c=c,
        d=d,
    )
    return {
        "test": test,
        "identities_with_temporal_pairs": identities_with_temporal_pairs,
        "identities_with_prior_caucasian_pairs": identities_with_prior_caucasian_pairs,
        "later_terms_after_prior_caucasian": _finalize_term_counts(later_terms_after_prior_caucasian),
    }


def build_next_paper_transition_summary(authors: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """Count each paper against the same author's next later analyzed paper."""
    a = b = c = d = 0
    identities_with_transitions = 0
    identities_with_prior_caucasian_transitions = 0
    later_terms_after_prior_caucasian = _empty_term_counts()

    for entry in authors.values():
        articles = _sorted_articles(entry)
        if len(articles) < 2:
            continue
        years = [article.pub_year for article in articles]
        identity_has_transition = False
        identity_has_prior_c_transition = False
        for article in articles:
            later_index = bisect_right(years, article.pub_year)
            if later_index >= len(articles):
                continue
            later = articles[later_index]
            identity_has_transition = True
            if article.caucasian and later.caucasian:
                a += 1
            elif article.caucasian and not later.caucasian:
                b += 1
            elif not article.caucasian and later.caucasian:
                c += 1
            else:
                d += 1
            if article.caucasian:
                identity_has_prior_c_transition = True
                _add_later_term_counts(later_terms_after_prior_caucasian, later)
        if identity_has_transition:
            identities_with_transitions += 1
        if identity_has_prior_c_transition:
            identities_with_prior_caucasian_transitions += 1

    test = _binary_test(
        test_id="next_later_paper_transitions",
        label="Caucasian use persists to the author's next later paper",
        unit="next later shared-author paper transitions",
        row1_label="Previous paper used Caucasian",
        row2_label="Previous paper did not use Caucasian",
        positive_label="Next later paper used Caucasian",
        negative_label="Next later paper did not use Caucasian",
        a=a,
        b=b,
        c=c,
        d=d,
    )
    return {
        "test": test,
        "identities_with_transitions": identities_with_transitions,
        "identities_with_prior_caucasian_transitions": identities_with_prior_caucasian_transitions,
        "later_terms_after_prior_caucasian": _finalize_term_counts(later_terms_after_prior_caucasian),
    }


def build_author_year_transition_summary(authors: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """Count transitions between adjacent active publication years for each author."""
    a = b = c = d = 0
    identities_with_year_transitions = 0
    identities_with_prior_caucasian_year_transitions = 0
    later_terms_after_prior_caucasian = _empty_term_counts()

    for entry in authors.values():
        articles = _sorted_articles(entry)
        by_year: dict[int, dict[str, bool]] = {}
        for article in articles:
            year_entry = by_year.setdefault(
                article.pub_year,
                {key: False for key in ("caucasian", "white", "european", "other", "any_term")}
            )
            for term in TERMS:
                year_entry[term] = year_entry[term] or bool(getattr(article, term))
            year_entry["any_term"] = year_entry["any_term"] or article.any_term
        years = sorted(by_year)
        if len(years) < 2:
            continue
        identities_with_year_transitions += 1
        for previous_year, next_year in zip(years, years[1:]):
            previous = by_year[previous_year]
            later = by_year[next_year]
            previous_caucasian = previous["caucasian"]
            later_caucasian = later["caucasian"]
            if previous_caucasian and later_caucasian:
                a += 1
            elif previous_caucasian and not later_caucasian:
                b += 1
            elif not previous_caucasian and later_caucasian:
                c += 1
            else:
                d += 1
            if previous_caucasian:
                identities_with_prior_caucasian_year_transitions += 1
                later_terms_after_prior_caucasian["denominator"] += 1
                for term in TERMS:
                    if later[term]:
                        later_terms_after_prior_caucasian[term] += 1
                if later["any_term"]:
                    later_terms_after_prior_caucasian["any_term"] += 1
                if later["white"] or later["european"]:
                    later_terms_after_prior_caucasian["white_or_european"] += 1
                if (later["white"] or later["european"]) and not later["caucasian"]:
                    later_terms_after_prior_caucasian["white_or_european_without_caucasian"] += 1

    test = _binary_test(
        test_id="adjacent_author_year_transitions",
        label="Caucasian use persists to the author's next active publication year",
        unit="adjacent author-year transitions",
        row1_label="Previous active year used Caucasian",
        row2_label="Previous active year did not use Caucasian",
        positive_label="Next active year used Caucasian",
        negative_label="Next active year did not use Caucasian",
        a=a,
        b=b,
        c=c,
        d=d,
    )
    return {
        "test": test,
        "identities_with_year_transitions": identities_with_year_transitions,
        "identities_with_prior_caucasian_year_transitions": identities_with_prior_caucasian_year_transitions,
        "later_terms_after_prior_caucasian": _finalize_term_counts(later_terms_after_prior_caucasian),
    }


def _build_cohort_stats(
    authors: Mapping[str, Mapping[str, Any]],
    articles: Mapping[int, ArticleRecord],
    article_author_ids: Mapping[int, set[str]],
    recent_start_year: int,
) -> dict[str, Any]:
    pre_caucasian_author_ids: set[str] = set()
    pre_never_caucasian_author_ids: set[str] = set()
    active_pre_caucasian_examples = []

    pre_c_active = pre_c_inactive = 0
    pre_never_active = pre_never_inactive = 0
    active_pre_c_recent_caucasian = 0
    active_pre_c_no_recent_caucasian = 0
    active_pre_never_recent_caucasian = 0
    active_pre_never_no_recent_caucasian = 0
    active_pre_c_recent_white_or_european = 0
    active_pre_c_no_recent_white_or_european = 0
    active_pre_never_recent_white_or_european = 0
    active_pre_never_no_recent_white_or_european = 0

    active_pre_c_recent_behavior = {
        "authors": 0,
        "recent_caucasian_authors": 0,
        "recent_white_authors": 0,
        "recent_european_authors": 0,
        "recent_white_or_european_authors": 0,
        "recent_white_or_european_without_caucasian_authors": 0,
        "recent_no_focal_term_authors": 0,
    }

    for identity, entry in authors.items():
        author_articles = _sorted_articles(entry)
        pre_articles = [article for article in author_articles if article.pub_year < recent_start_year]
        if not pre_articles:
            continue
        recent_articles = [article for article in author_articles if article.pub_year >= recent_start_year]
        pre_caucasian = any(article.caucasian for article in pre_articles)
        active_recent = bool(recent_articles)
        recent_caucasian = any(article.caucasian for article in recent_articles)
        recent_white = any(article.white for article in recent_articles)
        recent_european = any(article.european for article in recent_articles)
        recent_white_or_european = recent_white or recent_european
        recent_any_term = any(article.any_term for article in recent_articles)

        if pre_caucasian:
            pre_caucasian_author_ids.add(identity)
            if active_recent:
                pre_c_active += 1
                active_pre_c_recent_behavior["authors"] += 1
                active_pre_c_recent_behavior["recent_caucasian_authors"] += int(recent_caucasian)
                active_pre_c_recent_behavior["recent_white_authors"] += int(recent_white)
                active_pre_c_recent_behavior["recent_european_authors"] += int(recent_european)
                active_pre_c_recent_behavior["recent_white_or_european_authors"] += int(recent_white_or_european)
                active_pre_c_recent_behavior["recent_white_or_european_without_caucasian_authors"] += int(
                    recent_white_or_european and not recent_caucasian
                )
                active_pre_c_recent_behavior["recent_no_focal_term_authors"] += int(not recent_any_term)
                if recent_caucasian:
                    active_pre_c_recent_caucasian += 1
                else:
                    active_pre_c_no_recent_caucasian += 1
                if recent_white_or_european:
                    active_pre_c_recent_white_or_european += 1
                else:
                    active_pre_c_no_recent_white_or_european += 1
                if len(active_pre_caucasian_examples) < 20:
                    active_pre_caucasian_examples.append({
                        "identity": identity,
                        "display_name": entry.get("display_name") or identity,
                        "match_method": entry.get("match_method"),
                        "first_caucasian_year": min(article.pub_year for article in pre_articles if article.caucasian),
                        "recent_papers": len(recent_articles),
                        "recent_caucasian_papers": sum(1 for article in recent_articles if article.caucasian),
                        "recent_white_papers": sum(1 for article in recent_articles if article.white),
                        "recent_european_papers": sum(1 for article in recent_articles if article.european),
                        "latest_recent_year": max(article.pub_year for article in recent_articles),
                    })
            else:
                pre_c_inactive += 1
        else:
            pre_never_caucasian_author_ids.add(identity)
            if active_recent:
                pre_never_active += 1
                if recent_caucasian:
                    active_pre_never_recent_caucasian += 1
                else:
                    active_pre_never_no_recent_caucasian += 1
                if recent_white_or_european:
                    active_pre_never_recent_white_or_european += 1
                else:
                    active_pre_never_no_recent_white_or_european += 1
            else:
                pre_never_inactive += 1

    active_pre_caucasian_examples.sort(
        key=lambda item: (
            item["recent_caucasian_papers"],
            item["recent_white_papers"] + item["recent_european_papers"],
            item["recent_papers"],
        ),
        reverse=True,
    )

    recent_papers_by_pre_caucasian = _empty_term_counts()
    recent_papers_by_pre_never_caucasian = _empty_term_counts()
    recent_papers_without_matched_pre_author = 0
    for article in articles.values():
        if article.pub_year < recent_start_year:
            continue
        author_ids = article_author_ids.get(article.file_id, set())
        if author_ids & pre_caucasian_author_ids:
            _add_later_term_counts(recent_papers_by_pre_caucasian, article)
        elif author_ids & pre_never_caucasian_author_ids:
            _add_later_term_counts(recent_papers_by_pre_never_caucasian, article)
        else:
            recent_papers_without_matched_pre_author += 1

    tests = [
        _binary_test(
            test_id="pre_recent_caucasian_author_recent_activity",
            label="Pre-recent Caucasian authors remained active in recent years",
            unit="authors with at least one pre-recent analyzed paper",
            row1_label=f"Used Caucasian before {recent_start_year}",
            row2_label=f"Did not use Caucasian before {recent_start_year}",
            positive_label=f"Published again in {recent_start_year} or later",
            negative_label=f"No analyzed paper in {recent_start_year} or later",
            a=pre_c_active,
            b=pre_c_inactive,
            c=pre_never_active,
            d=pre_never_inactive,
        ),
        _binary_test(
            test_id="active_pre_recent_caucasian_author_recent_caucasian",
            label="Active pre-recent Caucasian authors used Caucasian in recent years",
            unit="authors active in recent years",
            row1_label=f"Used Caucasian before {recent_start_year}",
            row2_label=f"Did not use Caucasian before {recent_start_year}",
            positive_label=f"Used Caucasian in {recent_start_year} or later",
            negative_label=f"No Caucasian use in {recent_start_year} or later",
            a=active_pre_c_recent_caucasian,
            b=active_pre_c_no_recent_caucasian,
            c=active_pre_never_recent_caucasian,
            d=active_pre_never_no_recent_caucasian,
        ),
        _binary_test(
            test_id="active_pre_recent_caucasian_author_recent_white_or_european",
            label="Active pre-recent Caucasian authors used white or European in recent years",
            unit="authors active in recent years",
            row1_label=f"Used Caucasian before {recent_start_year}",
            row2_label=f"Did not use Caucasian before {recent_start_year}",
            positive_label=f"Used white or European in {recent_start_year} or later",
            negative_label=f"No white or European use in {recent_start_year} or later",
            a=active_pre_c_recent_white_or_european,
            b=active_pre_c_no_recent_white_or_european,
            c=active_pre_never_recent_white_or_european,
            d=active_pre_never_no_recent_white_or_european,
        ),
    ]

    return {
        "recent_start_year": recent_start_year,
        "pre_caucasian_authors": len(pre_caucasian_author_ids),
        "pre_never_caucasian_authors": len(pre_never_caucasian_author_ids),
        "active_pre_caucasian_recent_behavior": active_pre_c_recent_behavior,
        "recent_papers_by_pre_caucasian_author": _finalize_term_counts(recent_papers_by_pre_caucasian),
        "recent_papers_by_pre_never_caucasian_author": _finalize_term_counts(recent_papers_by_pre_never_caucasian),
        "recent_papers_without_matched_pre_author": recent_papers_without_matched_pre_author,
        "tests": tests,
        "active_pre_caucasian_examples": active_pre_caucasian_examples,
    }


def _build_annual_decomposition(
    articles: Mapping[int, ArticleRecord],
    authors: Mapping[str, Mapping[str, Any]],
    article_author_ids: Mapping[int, set[str]],
) -> list[dict[str, Any]]:
    author_first_caucasian_year: dict[str, int] = {}
    for identity, entry in authors.items():
        caucasian_years = [
            article.pub_year
            for article in entry.get("articles", {}).values()
            if article.caucasian
        ]
        if caucasian_years:
            author_first_caucasian_year[identity] = min(caucasian_years)

    annual = defaultdict(lambda: {
        "year": None,
        "processed_articles": 0,
        "caucasian_count": 0,
        "white_count": 0,
        "european_count": 0,
        "other_count": 0,
        "articles_with_matched_author": 0,
        "articles_without_matched_author": 0,
        "papers_with_prior_caucasian_author": 0,
        "prior_caucasian_author_caucasian_count": 0,
        "prior_caucasian_author_white_count": 0,
        "prior_caucasian_author_european_count": 0,
        "prior_caucasian_author_white_or_european_without_caucasian_count": 0,
        "papers_without_prior_caucasian_author": 0,
        "without_prior_caucasian_author_caucasian_count": 0,
        "without_prior_caucasian_author_white_count": 0,
        "without_prior_caucasian_author_european_count": 0,
    })

    for article in articles.values():
        row = annual[article.pub_year]
        row["year"] = article.pub_year
        row["processed_articles"] += 1
        for term in TERMS:
            if getattr(article, term):
                row[f"{term}_count"] += 1
        author_ids = article_author_ids.get(article.file_id, set())
        if author_ids:
            row["articles_with_matched_author"] += 1
        else:
            row["articles_without_matched_author"] += 1
        has_prior_caucasian_author = any(
            author_first_caucasian_year.get(identity, math.inf) < article.pub_year
            for identity in author_ids
        )
        if has_prior_caucasian_author:
            row["papers_with_prior_caucasian_author"] += 1
            if article.caucasian:
                row["prior_caucasian_author_caucasian_count"] += 1
            if article.white:
                row["prior_caucasian_author_white_count"] += 1
            if article.european:
                row["prior_caucasian_author_european_count"] += 1
            if article.white_or_european_without_caucasian:
                row["prior_caucasian_author_white_or_european_without_caucasian_count"] += 1
        else:
            row["papers_without_prior_caucasian_author"] += 1
            if article.caucasian:
                row["without_prior_caucasian_author_caucasian_count"] += 1
            if article.white:
                row["without_prior_caucasian_author_white_count"] += 1
            if article.european:
                row["without_prior_caucasian_author_european_count"] += 1

    result = []
    for year in sorted(annual):
        row = dict(annual[year])
        row["caucasian_rate"] = _rate(row["caucasian_count"], row["processed_articles"])
        row["prior_caucasian_author_share"] = _rate(
            row["papers_with_prior_caucasian_author"],
            row["articles_with_matched_author"],
        )
        row["prior_caucasian_author_caucasian_rate"] = _rate(
            row["prior_caucasian_author_caucasian_count"],
            row["papers_with_prior_caucasian_author"],
        )
        row["prior_caucasian_author_white_or_european_without_caucasian_rate"] = _rate(
            row["prior_caucasian_author_white_or_european_without_caucasian_count"],
            row["papers_with_prior_caucasian_author"],
        )
        result.append(row)
    return result


def _build_author_usage_profiles(authors: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    profiles = []
    for identity, entry in authors.items():
        author_articles = _sorted_articles(entry)
        if not author_articles:
            continue
        caucasian_years = [article.pub_year for article in author_articles if article.caucasian]
        white_count = sum(1 for article in author_articles if article.white)
        european_count = sum(1 for article in author_articles if article.european)
        caucasian_count = len(caucasian_years)
        if not (caucasian_count or white_count or european_count):
            continue
        profiles.append({
            "identity": identity,
            "display_name": entry.get("display_name") or identity,
            "match_method": entry.get("match_method"),
            "article_count": len(author_articles),
            "first_article_year": min(article.pub_year for article in author_articles),
            "last_article_year": max(article.pub_year for article in author_articles),
            "caucasian_count": caucasian_count,
            "white_count": white_count,
            "european_count": european_count,
            "first_caucasian_year": min(caucasian_years) if caucasian_years else None,
            "last_caucasian_year": max(caucasian_years) if caucasian_years else None,
        })
    profiles.sort(key=lambda item: (
        item["first_caucasian_year"] if item["first_caucasian_year"] is not None else 9999,
        item["last_caucasian_year"] if item["last_caucasian_year"] is not None else 9999,
        item["display_name"],
    ))
    return profiles


def _build_term_shift_tests(
    articles: Mapping[int, ArticleRecord],
    peak_year: int,
    recent_start_year: int,
) -> dict[str, Any]:
    peak_articles = [article for article in articles.values() if article.pub_year == peak_year]
    recent_articles = [article for article in articles.values() if article.pub_year >= recent_start_year]

    def period_counts(period_articles: list[ArticleRecord]) -> dict[str, int]:
        return {
            "articles": len(period_articles),
            "caucasian": sum(1 for article in period_articles if article.caucasian),
            "white": sum(1 for article in period_articles if article.white),
            "european": sum(1 for article in period_articles if article.european),
            "white_or_european_without_caucasian": sum(
                1 for article in period_articles if article.white_or_european_without_caucasian
            ),
        }

    peak = period_counts(peak_articles)
    recent = period_counts(recent_articles)
    chi_square, chi_square_p, df = _chi_square_test_table([
        [peak["caucasian"], peak["white"], peak["european"]],
        [recent["caucasian"], recent["white"], recent["european"]],
    ])

    term_distribution_test = {
        "test_id": "peak_vs_recent_term_distribution",
        "label": f"Term distribution differs between {peak_year} and {recent_start_year}+",
        "unit": "term-positive article counts; categories may overlap",
        "row1_label": str(peak_year),
        "row2_label": f"{recent_start_year}+",
        "columns": ["caucasian", "white", "european"],
        "table": [
            [peak["caucasian"], peak["white"], peak["european"]],
            [recent["caucasian"], recent["white"], recent["european"]],
        ],
        "chi_square": chi_square,
        "chi_square_df": df,
        "chi_square_p": chi_square_p,
    }

    decline_test = _binary_test(
        test_id="peak_vs_recent_caucasian_rate",
        label=f"Caucasian article rate fell from {peak_year} to {recent_start_year}+",
        unit="analyzed articles",
        row1_label=str(peak_year),
        row2_label=f"{recent_start_year}+",
        positive_label="Article used Caucasian",
        negative_label="Article did not use Caucasian",
        a=peak["caucasian"],
        b=peak["articles"] - peak["caucasian"],
        c=recent["caucasian"],
        d=recent["articles"] - recent["caucasian"],
    )

    def cooccurrence(term_a: str, term_b: str) -> dict[str, Any]:
        a = sum(1 for article in articles.values() if getattr(article, term_a) and getattr(article, term_b))
        b = sum(1 for article in articles.values() if getattr(article, term_a) and not getattr(article, term_b))
        c = sum(1 for article in articles.values() if not getattr(article, term_a) and getattr(article, term_b))
        d = sum(1 for article in articles.values() if not getattr(article, term_a) and not getattr(article, term_b))
        return _binary_test(
            test_id=f"article_level_{term_a}_vs_{term_b}_cooccurrence",
            label=f"Article-level {term_a} and {term_b} co-occurrence differs from independence",
            unit="analyzed articles",
            row1_label=f"Article used {term_a}",
            row2_label=f"Article did not use {term_a}",
            positive_label=f"Article used {term_b}",
            negative_label=f"Article did not use {term_b}",
            a=a,
            b=b,
            c=c,
            d=d,
        )

    return {
        "peak_year": peak_year,
        "recent_start_year": recent_start_year,
        "peak_counts": peak,
        "recent_counts": recent,
        "term_distribution_test": term_distribution_test,
        "decline_test": decline_test,
        "cooccurrence_tests": [
            cooccurrence("caucasian", "white"),
            cooccurrence("caucasian", "european"),
            cooccurrence("white", "european"),
        ],
    }


def _build_legacy_reduction_decomposition(
    articles: Mapping[int, ArticleRecord],
    authors: Mapping[str, Mapping[str, Any]],
    article_author_ids: Mapping[int, set[str]],
    peak_year: int,
    recent_start_year: int,
) -> dict[str, Any]:
    legacy_caucasian_author_ids = {
        identity
        for identity, entry in authors.items()
        if any(
            article.caucasian and article.pub_year <= peak_year
            for article in entry.get("articles", {}).values()
        )
    }
    author_first_year = {
        identity: min(article.pub_year for article in entry.get("articles", {}).values())
        for identity, entry in authors.items()
        if entry.get("articles")
    }
    author_ever_caucasian = {
        identity
        for identity, entry in authors.items()
        if any(article.caucasian for article in entry.get("articles", {}).values())
    }

    def summarize(period_label: str, period_articles: list[ArticleRecord]) -> dict[str, Any]:
        total = len(period_articles)
        caucasian = sum(1 for article in period_articles if article.caucasian)
        legacy_total = 0
        legacy_caucasian = 0
        nonlegacy_total = 0
        nonlegacy_caucasian = 0
        new_never_caucasian_author_papers = 0
        for article in period_articles:
            ids = article_author_ids.get(article.file_id, set())
            has_legacy = bool(ids & legacy_caucasian_author_ids)
            has_new_never_caucasian = any(
                author_first_year.get(identity, 0) > peak_year
                and identity not in author_ever_caucasian
                for identity in ids
            )
            if has_legacy:
                legacy_total += 1
                legacy_caucasian += int(article.caucasian)
            else:
                nonlegacy_total += 1
                nonlegacy_caucasian += int(article.caucasian)
            new_never_caucasian_author_papers += int(has_new_never_caucasian)
        return {
            "period": period_label,
            "articles": total,
            "caucasian": caucasian,
            "caucasian_rate": _rate(caucasian, total),
            "legacy_author_articles": legacy_total,
            "legacy_author_share": _rate(legacy_total, total),
            "legacy_author_caucasian": legacy_caucasian,
            "legacy_author_caucasian_rate": _rate(legacy_caucasian, legacy_total),
            "nonlegacy_author_articles": nonlegacy_total,
            "nonlegacy_author_caucasian": nonlegacy_caucasian,
            "nonlegacy_author_caucasian_rate": _rate(nonlegacy_caucasian, nonlegacy_total),
            "new_never_caucasian_author_papers": new_never_caucasian_author_papers,
            "new_never_caucasian_author_paper_share": _rate(new_never_caucasian_author_papers, total),
        }

    peak_articles = [article for article in articles.values() if article.pub_year == peak_year]
    recent_articles = [article for article in articles.values() if article.pub_year >= recent_start_year]
    peak = summarize(str(peak_year), peak_articles)
    recent = summarize(f"{recent_start_year}+", recent_articles)

    s0 = peak["legacy_author_share"] or 0.0
    s1 = recent["legacy_author_share"] or 0.0
    r0 = peak["legacy_author_caucasian_rate"] or 0.0
    r1 = recent["legacy_author_caucasian_rate"] or 0.0
    n0 = peak["nonlegacy_author_caucasian_rate"] or 0.0
    n1 = recent["nonlegacy_author_caucasian_rate"] or 0.0
    overall0 = peak["caucasian_rate"] or 0.0
    overall1 = recent["caucasian_rate"] or 0.0

    behavior_component = 0.5 * (
        s0 * (r1 - r0) + (1 - s0) * (n1 - n0)
        + s1 * (r1 - r0) + (1 - s1) * (n1 - n0)
    )
    composition_component = 0.5 * (
        (s1 - s0) * (r0 - n0)
        + (s1 - s0) * (r1 - n1)
    )
    total_change = overall1 - overall0
    total_reduction = -total_change if total_change < 0 else total_change
    behavior_reduction = -behavior_component if total_change < 0 else behavior_component
    composition_reduction = -composition_component if total_change < 0 else composition_component

    return {
        "peak_year": peak_year,
        "recent_start_year": recent_start_year,
        "legacy_author_definition": f"matched author with at least one Caucasian-positive article through {peak_year}",
        "legacy_caucasian_authors": len(legacy_caucasian_author_ids),
        "peak": peak,
        "recent": recent,
        "total_rate_change": total_change,
        "behavior_component": behavior_component,
        "composition_component": composition_component,
        "behavior_reduction_share": _rate(behavior_reduction, total_reduction),
        "composition_reduction_share": _rate(composition_reduction, total_reduction),
    }


def build_author_persistence_statistics(
    rows: Iterable[Mapping[str, Any]],
    recent_start_year: int = DEFAULT_RECENT_START_YEAR,
    peak_year: int = DEFAULT_PEAK_YEAR,
) -> dict[str, Any]:
    dataset = prepare_author_dataset(rows)
    authors = dataset["authors"]
    articles = dataset["articles"]
    article_author_ids = dataset["article_author_ids"]

    all_temporal_summary = build_all_temporal_pair_summary(authors)
    next_paper_summary = build_next_paper_transition_summary(authors)
    author_year_summary = build_author_year_transition_summary(authors)
    cohort = _build_cohort_stats(authors, articles, article_author_ids, recent_start_year)
    annual = _build_annual_decomposition(articles, authors, article_author_ids)
    author_usage_profiles = _build_author_usage_profiles(authors)
    term_shift_tests = _build_term_shift_tests(articles, peak_year, recent_start_year)
    legacy_decomposition = _build_legacy_reduction_decomposition(
        articles,
        authors,
        article_author_ids,
        peak_year,
        recent_start_year,
    )

    population = dict(dataset["population"])
    population["recent_start_year"] = recent_start_year
    population["peak_year"] = peak_year
    population["author_identities_with_any_caucasian"] = sum(
        1
        for entry in authors.values()
        if any(article.caucasian for article in entry.get("articles", {}).values())
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "population": population,
        "predictive_tests": [
            all_temporal_summary["test"],
            next_paper_summary["test"],
            author_year_summary["test"],
        ],
        "all_temporal_pairs": all_temporal_summary,
        "next_paper_transitions": next_paper_summary,
        "author_year_transitions": author_year_summary,
        "cohort": cohort,
        "annual": annual,
        "author_usage_profiles": author_usage_profiles,
        "term_shift_tests": term_shift_tests,
        "legacy_reduction_decomposition": legacy_decomposition,
        "method": {
            "author_matching": (
                "Author identities are matched by ORCID when present. Otherwise a strict normalized "
                "family plus non-initial-only given-name key is used. Initial-only names without ORCID "
                "are excluded to reduce false same-author matches."
            ),
            "pair_unit": (
                "Pair tests count shared-author temporal paper pairs. If two papers share multiple "
                "matched authors, each shared author contributes a separate author-paper pair."
            ),
            "primary_tests": (
                "Two-sided Fisher exact tests are reported with Pearson chi-square checks and "
                "Haldane-adjusted odds ratios for 2x2 tables."
            ),
            "recent_start_year": recent_start_year,
            "peak_year": peak_year,
        },
    }


def format_p_value(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "N/A"
    if value < 1e-6:
        return "< 1e-6"
    if value < 0.0001:
        return f"{value:.2e}"
    return f"{value:.4f}"


def format_rate(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "N/A"
    return f"{value * 100:.2f}%"


def _format_risk_difference(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "N/A"
    return f"{value * 100:+.2f} pp"


def _format_odds_ratio(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "N/A"
    return f"{value:.3f}"


def _render_test_rows(tests: Iterable[Mapping[str, Any]]) -> str:
    rows = []
    for test in tests:
        row1_total = test["row1_positive"] + test["row1_negative"]
        row2_total = test["row2_positive"] + test["row2_negative"]
        rows.append(f"""
                <tr>
                    <td>{html.escape(str(test["label"]))}</td>
                    <td>{html.escape(str(test["unit"]))}</td>
                    <td>{html.escape(str(test["row1_label"]))}</td>
                    <td class="numeric">{test["row1_positive"]:,} / {row1_total:,}</td>
                    <td class="numeric">{format_rate(test["row1_rate"])}</td>
                    <td>{html.escape(str(test["row2_label"]))}</td>
                    <td class="numeric">{test["row2_positive"]:,} / {row2_total:,}</td>
                    <td class="numeric">{format_rate(test["row2_rate"])}</td>
                    <td class="numeric">{_format_risk_difference(test["risk_difference"])}</td>
                    <td class="numeric">{_format_odds_ratio(test["odds_ratio_haldane"])}</td>
                    <td class="numeric">{format_p_value(test["fisher_exact_p"])}</td>
                </tr>""")
    return "".join(rows)


def _render_decomposition_table(decomposition: Mapping[str, Any]) -> str:
    peak = decomposition["peak"]
    recent = decomposition["recent"]
    behavior_share = decomposition.get("behavior_reduction_share")
    composition_share = decomposition.get("composition_reduction_share")
    return f"""
        <table>
            <thead>
                <tr>
                    <th>Period</th>
                    <th>Articles</th>
                    <th>Caucasian Rate</th>
                    <th>Legacy-Caucasian-Author Share</th>
                    <th>Caucasian Rate Among Legacy Authors</th>
                    <th>Caucasian Rate Among Other Authors</th>
                    <th>New Never-Caucasian Author Paper Share</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>{html.escape(str(peak['period']))}</td>
                    <td class="numeric">{peak['articles']:,}</td>
                    <td class="numeric">{format_rate(peak.get('caucasian_rate'))}</td>
                    <td class="numeric">{format_rate(peak.get('legacy_author_share'))}</td>
                    <td class="numeric">{format_rate(peak.get('legacy_author_caucasian_rate'))}</td>
                    <td class="numeric">{format_rate(peak.get('nonlegacy_author_caucasian_rate'))}</td>
                    <td class="numeric">{format_rate(peak.get('new_never_caucasian_author_paper_share'))}</td>
                </tr>
                <tr>
                    <td>{html.escape(str(recent['period']))}</td>
                    <td class="numeric">{recent['articles']:,}</td>
                    <td class="numeric">{format_rate(recent.get('caucasian_rate'))}</td>
                    <td class="numeric">{format_rate(recent.get('legacy_author_share'))}</td>
                    <td class="numeric">{format_rate(recent.get('legacy_author_caucasian_rate'))}</td>
                    <td class="numeric">{format_rate(recent.get('nonlegacy_author_caucasian_rate'))}</td>
                    <td class="numeric">{format_rate(recent.get('new_never_caucasian_author_paper_share'))}</td>
                </tr>
            </tbody>
        </table>
        <p class="method-note">
            Symmetric decomposition of the overall Caucasian-rate change:
            behavior/stopping component {format_rate(behavior_share)};
            author-composition/new-author component {format_rate(composition_share)}.
            Legacy authors are {html.escape(str(decomposition.get('legacy_author_definition', '')))}.
        </p>
"""


def _render_term_shift_table(term_shift_tests: Mapping[str, Any]) -> str:
    distribution = term_shift_tests["term_distribution_test"]
    decline = term_shift_tests["decline_test"]
    cooccurrence_rows = _render_test_rows(term_shift_tests["cooccurrence_tests"])
    peak_counts = term_shift_tests["peak_counts"]
    recent_counts = term_shift_tests["recent_counts"]
    chi_text = "N/A" if distribution.get("chi_square") is None else f"{distribution['chi_square']:.3f}"
    return f"""
        <table>
            <thead>
                <tr>
                    <th>Period</th>
                    <th>Articles</th>
                    <th>Caucasian</th>
                    <th>White</th>
                    <th>European</th>
                    <th>White/European Without Caucasian</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>{term_shift_tests['peak_year']}</td>
                    <td class="numeric">{peak_counts['articles']:,}</td>
                    <td class="numeric">{peak_counts['caucasian']:,}</td>
                    <td class="numeric">{peak_counts['white']:,}</td>
                    <td class="numeric">{peak_counts['european']:,}</td>
                    <td class="numeric">{peak_counts['white_or_european_without_caucasian']:,}</td>
                </tr>
                <tr>
                    <td>{term_shift_tests['recent_start_year']}+</td>
                    <td class="numeric">{recent_counts['articles']:,}</td>
                    <td class="numeric">{recent_counts['caucasian']:,}</td>
                    <td class="numeric">{recent_counts['white']:,}</td>
                    <td class="numeric">{recent_counts['european']:,}</td>
                    <td class="numeric">{recent_counts['white_or_european_without_caucasian']:,}</td>
                </tr>
            </tbody>
        </table>
        <p class="method-note">
            Term-distribution chi-square test over Caucasian/white/European counts:
            X2={chi_text}, df={distribution['chi_square_df']},
            p={format_p_value(distribution['chi_square_p'])}.
            Caucasian-rate decline test: OR={_format_odds_ratio(decline['odds_ratio_haldane'])},
            Fisher p={format_p_value(decline['fisher_exact_p'])}.
        </p>
        <table>
            <thead>
                <tr>
                    <th>Co-Occurrence Test</th>
                    <th>Unit</th>
                    <th>Exposed Group</th>
                    <th>Exposed Positive</th>
                    <th>Exposed Rate</th>
                    <th>Comparison Group</th>
                    <th>Comparison Positive</th>
                    <th>Comparison Rate</th>
                    <th>Risk Difference</th>
                    <th>Odds Ratio</th>
                    <th>Fisher p</th>
                </tr>
            </thead>
            <tbody>
{cooccurrence_rows}
            </tbody>
        </table>
"""


def render_author_persistence_dashboard_section(stats: Mapping[str, Any]) -> str:
    population = stats["population"]
    cohort = stats["cohort"]
    decomposition = stats["legacy_reduction_decomposition"]
    term_shift = stats["term_shift_tests"]["term_distribution_test"]
    behavior = cohort["active_pre_caucasian_recent_behavior"]
    active_pre_c = behavior["authors"]
    substitution_authors = behavior["recent_white_or_european_without_caucasian_authors"]
    substitution_rate = _rate(substitution_authors, active_pre_c)
    all_pairs = stats["all_temporal_pairs"]["test"]
    next_pairs = stats["next_paper_transitions"]["test"]
    cohort_tests = cohort["tests"]

    return f"""
        <h2>Shared-Author Caucasian Persistence</h2>
        <div class="grid">
            <div class="card">
                <h3>Matched Author Identities</h3>
                <div class="value">{population['author_identities']:,}</div>
                <div class="subvalue">{population['articles_with_matched_author']:,} analyzed articles have at least one matched author</div>
            </div>
            <div class="card">
                <h3>Temporal Author-Paper Pairs</h3>
                <div class="value">{(all_pairs['row1_positive'] + all_pairs['row1_negative'] + all_pairs['row2_positive'] + all_pairs['row2_negative']):,}</div>
                <div class="subvalue">Earlier-to-later pairs sharing a matched author</div>
            </div>
            <div class="card">
                <h3>Active Previous Users</h3>
                <div class="value">{active_pre_c:,}</div>
                <div class="subvalue">Authors who used Caucasian before {cohort['recent_start_year']} and published again</div>
            </div>
            <div class="card">
                <h3>Recent Substitution Signal</h3>
                <div class="value">{format_rate(substitution_rate)}</div>
                <div class="subvalue">{substitution_authors:,} active previous users used white/European but not Caucasian recently</div>
            </div>
            <div class="card">
                <h3>Stopping Component</h3>
                <div class="value">{format_rate(decomposition.get('behavior_reduction_share'))}</div>
                <div class="subvalue">Share of peak-to-recent reduction from lower use within author groups</div>
            </div>
            <div class="card">
                <h3>New-Author Component</h3>
                <div class="value">{format_rate(decomposition.get('composition_reduction_share'))}</div>
                <div class="subvalue">Share of reduction from changing author composition</div>
            </div>
            <div class="card">
                <h3>Term Shift Test</h3>
                <div class="value">{format_p_value(term_shift.get('chi_square_p'))}</div>
                <div class="subvalue">Chi-square p for {population['peak_year']} vs {population['recent_start_year']}+ term distribution</div>
            </div>
        </div>
        <p class="method-note">
            Author matching uses ORCID when available, otherwise strict normalized full-name keys; initial-only names without ORCID are excluded.
            Full report: <a href="author_persistence.html">HTML</a>.
            Machine-readable output is written to <code>author_persistence.json</code>, <code>author_persistence.csv</code>, and <code>author_persistence_annual.csv</code>.
        </p>
        <table>
            <thead>
                <tr>
                    <th>Test</th>
                    <th>Unit</th>
                    <th>Exposed Group</th>
                    <th>Exposed Positive</th>
                    <th>Exposed Rate</th>
                    <th>Comparison Group</th>
                    <th>Comparison Positive</th>
                    <th>Comparison Rate</th>
                    <th>Risk Difference</th>
                    <th>Odds Ratio</th>
                    <th>Fisher p</th>
                </tr>
            </thead>
            <tbody>
{_render_test_rows([all_pairs, next_pairs] + cohort_tests)}
            </tbody>
        </table>
"""


def render_author_persistence_html(stats: Mapping[str, Any]) -> str:
    population = stats["population"]
    generated_at = html.escape(str(stats.get("generated_at", "")))
    method = stats.get("method", {})
    cohort = stats["cohort"]
    decomposition = stats["legacy_reduction_decomposition"]
    term_shift_tests = stats["term_shift_tests"]
    behavior = cohort["active_pre_caucasian_recent_behavior"]
    all_tests = list(stats["predictive_tests"]) + list(cohort["tests"])
    author_usage_profiles = stats.get("author_usage_profiles", [])

    term_rows = []
    for label, term_stats in (
        ("All temporal shared-author paper pairs", stats["all_temporal_pairs"]["later_terms_after_prior_caucasian"]),
        ("Next later paper transitions", stats["next_paper_transitions"]["later_terms_after_prior_caucasian"]),
        ("Adjacent author-year transitions", stats["author_year_transitions"]["later_terms_after_prior_caucasian"]),
    ):
        denominator = term_stats["denominator"]
        term_rows.append(f"""
                <tr>
                    <td>{html.escape(label)}</td>
                    <td class="numeric">{denominator:,}</td>
                    <td class="numeric">{term_stats['caucasian']:,} ({format_rate(term_stats.get('caucasian_rate'))})</td>
                    <td class="numeric">{term_stats['white']:,} ({format_rate(term_stats.get('white_rate'))})</td>
                    <td class="numeric">{term_stats['european']:,} ({format_rate(term_stats.get('european_rate'))})</td>
                    <td class="numeric">{term_stats['white_or_european_without_caucasian']:,} ({format_rate(term_stats.get('white_or_european_without_caucasian_rate'))})</td>
                </tr>""")

    recent_pre_c_papers = cohort["recent_papers_by_pre_caucasian_author"]
    recent_pre_never_papers = cohort["recent_papers_by_pre_never_caucasian_author"]
    annual_rows = []
    for row in stats["annual"][-20:]:
        annual_rows.append(f"""
                <tr>
                    <td>{row['year']}</td>
                    <td class="numeric">{row['processed_articles']:,}</td>
                    <td class="numeric">{row['caucasian_count']:,} ({format_rate(row.get('caucasian_rate'))})</td>
                    <td class="numeric">{row['articles_with_matched_author']:,}</td>
                    <td class="numeric">{row['papers_with_prior_caucasian_author']:,} ({format_rate(row.get('prior_caucasian_author_share'))})</td>
                    <td class="numeric">{row['prior_caucasian_author_caucasian_count']:,} ({format_rate(row.get('prior_caucasian_author_caucasian_rate'))})</td>
                    <td class="numeric">{row['prior_caucasian_author_white_or_european_without_caucasian_count']:,} ({format_rate(row.get('prior_caucasian_author_white_or_european_without_caucasian_rate'))})</td>
                </tr>""")

    example_rows = []
    for item in cohort.get("active_pre_caucasian_examples", [])[:10]:
        example_rows.append(f"""
                <tr>
                    <td>{html.escape(str(item.get('display_name') or item.get('identity') or ''))}</td>
                    <td>{html.escape(str(item.get('match_method') or ''))}</td>
                    <td class="numeric">{item.get('first_caucasian_year') or ''}</td>
                    <td class="numeric">{item.get('recent_papers') or 0}</td>
                    <td class="numeric">{item.get('recent_caucasian_papers') or 0}</td>
                    <td class="numeric">{item.get('recent_white_papers') or 0}</td>
                    <td class="numeric">{item.get('recent_european_papers') or 0}</td>
                    <td class="numeric">{item.get('latest_recent_year') or ''}</td>
                </tr>""")
    if not example_rows:
        example_rows.append("""<tr><td colspan="8">No active pre-recent Caucasian authors were found.</td></tr>""")

    annual_scatter_data = []
    for row in stats["annual"]:
        annual_scatter_data.append({
            "year": row["year"],
            "caucasian_pct": (row.get("caucasian_rate") or 0) * 100,
            "white_pct": _rate(row.get("white_count", 0), row.get("processed_articles", 0)) * 100
            if row.get("processed_articles") else 0,
            "european_pct": _rate(row.get("european_count", 0), row.get("processed_articles", 0)) * 100
            if row.get("processed_articles") else 0,
        })
    caucasian_span_data = [
        profile for profile in author_usage_profiles
        if profile.get("first_caucasian_year") is not None
    ]
    author_usage_scatter_data = [
        {
            "display_name": profile.get("display_name"),
            "match_method": profile.get("match_method"),
            "article_count": profile.get("article_count"),
            "caucasian_count": profile.get("caucasian_count"),
            "white_count": profile.get("white_count"),
            "european_count": profile.get("european_count"),
        }
        for profile in author_usage_profiles
    ]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Shared-Author Caucasian Persistence</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            margin: 0;
            padding: 2rem;
            background: #f6f7fb;
            color: #1f2933;
        }}
        main {{
            max-width: 1250px;
            margin: 0 auto;
            background: white;
            padding: 2rem;
            border-radius: 12px;
            box-shadow: 0 2px 12px rgba(15, 23, 42, 0.08);
        }}
        h1, h2 {{
            color: #102a43;
        }}
        .meta, .method-note {{
            color: #52606d;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1rem;
            margin: 1.5rem 0;
        }}
        .card {{
            background: #f0f4f8;
            border-radius: 10px;
            padding: 1rem;
        }}
        .value {{
            font-size: 2rem;
            font-weight: 700;
            color: #0b4f6c;
        }}
        .subvalue {{
            color: #627d98;
            margin-top: 0.25rem;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 1rem 0 2rem;
            background: white;
        }}
        th, td {{
            border-bottom: 1px solid #d9e2ec;
            padding: 0.65rem;
            text-align: left;
            vertical-align: top;
        }}
        th {{
            background: #e6f6ff;
            color: #102a43;
        }}
        .numeric {{
            text-align: right;
            white-space: nowrap;
            font-variant-numeric: tabular-nums;
        }}
        a {{
            color: #0967d2;
        }}
        .chart-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
            gap: 1rem;
            margin: 1rem 0 2rem;
        }}
        .chart-container {{
            background: #f8fbff;
            border: 1px solid #d9e2ec;
            border-radius: 10px;
            padding: 1rem;
        }}
        .chart-container h2 {{
            font-size: 1rem;
            margin-top: 0;
        }}
        canvas {{
            max-height: 380px;
        }}
    </style>
</head>
<body>
<main>
    <p><a href="index.html">Back to dashboard</a></p>
    <h1>Shared-Author Caucasian Persistence</h1>
    <p class="meta">Generated: {generated_at}</p>
    <div class="grid">
        <div class="card">
            <h2>Analyzed Articles</h2>
            <div class="value">{population['analyzed_articles']:,}</div>
            <div class="subvalue">{population['articles_with_matched_author']:,} have at least one matched author</div>
        </div>
        <div class="card">
            <h2>Author Identities</h2>
            <div class="value">{population['author_identities']:,}</div>
            <div class="subvalue">{population['author_identities_with_any_caucasian']:,} have at least one Caucasian-positive article</div>
        </div>
        <div class="card">
            <h2>Pre-Recent Users</h2>
            <div class="value">{cohort['pre_caucasian_authors']:,}</div>
            <div class="subvalue">Used Caucasian before {cohort['recent_start_year']}</div>
        </div>
        <div class="card">
            <h2>Active Previous Users</h2>
            <div class="value">{behavior['authors']:,}</div>
            <div class="subvalue">Published again in {cohort['recent_start_year']} or later</div>
        </div>
    </div>
    <p class="method-note">
        Author matching: {html.escape(str(method.get("author_matching", "")))}
        Pair unit: {html.escape(str(method.get("pair_unit", "")))}
        Tests: {html.escape(str(method.get("primary_tests", "")))}
        Machine-readable outputs: <a href="author_persistence.json">JSON</a>,
        <a href="author_persistence.csv">CSV</a>, and
        <a href="author_persistence_annual.csv">annual CSV</a>.
    </p>

    <h2>Reduction Decomposition</h2>
{_render_decomposition_table(decomposition)}

    <h2>Scatter Plots</h2>
    <div class="chart-grid">
        <div class="chart-container">
            <h2>First vs Last Caucasian Use by Author</h2>
            <canvas id="caucasianSpanChart"></canvas>
        </div>
        <div class="chart-container">
            <h2>Annual Caucasian vs White Rates</h2>
            <canvas id="annualCaucasianWhiteChart"></canvas>
        </div>
        <div class="chart-container">
            <h2>Annual Caucasian vs European Rates</h2>
            <canvas id="annualCaucasianEuropeanChart"></canvas>
        </div>
        <div class="chart-container">
            <h2>Author Caucasian vs White Counts</h2>
            <canvas id="authorCaucasianWhiteChart"></canvas>
        </div>
        <div class="chart-container">
            <h2>Author Caucasian vs European Counts</h2>
            <canvas id="authorCaucasianEuropeanChart"></canvas>
        </div>
    </div>

    <h2>Interchangeability / Random-Chance Checks</h2>
{_render_term_shift_table(term_shift_tests)}

    <h2>2x2 Tests</h2>
    <table>
        <thead>
            <tr>
                <th>Test</th>
                <th>Unit</th>
                <th>Exposed Group</th>
                <th>Exposed Positive</th>
                <th>Exposed Rate</th>
                <th>Comparison Group</th>
                <th>Comparison Positive</th>
                <th>Comparison Rate</th>
                <th>Risk Difference</th>
                <th>Odds Ratio</th>
                <th>Fisher p</th>
            </tr>
        </thead>
        <tbody>
{_render_test_rows(all_tests)}
        </tbody>
    </table>

    <h2>Later Terminology After Prior Caucasian Use</h2>
    <table>
        <thead>
            <tr>
                <th>Unit</th>
                <th>Denominator</th>
                <th>Caucasian Again</th>
                <th>White</th>
                <th>European</th>
                <th>White/European Without Caucasian</th>
            </tr>
        </thead>
        <tbody>
{''.join(term_rows)}
        </tbody>
    </table>

    <h2>Recent Behavior of Authors Who Used Caucasian Before {cohort['recent_start_year']}</h2>
    <div class="grid">
        <div class="card">
            <h2>Recent Caucasian</h2>
            <div class="value">{behavior['recent_caucasian_authors']:,}</div>
            <div class="subvalue">{format_rate(_rate(behavior['recent_caucasian_authors'], behavior['authors']))} of active previous users</div>
        </div>
        <div class="card">
            <h2>Recent White/European</h2>
            <div class="value">{behavior['recent_white_or_european_authors']:,}</div>
            <div class="subvalue">{format_rate(_rate(behavior['recent_white_or_european_authors'], behavior['authors']))} of active previous users</div>
        </div>
        <div class="card">
            <h2>White/European, No Caucasian</h2>
            <div class="value">{behavior['recent_white_or_european_without_caucasian_authors']:,}</div>
            <div class="subvalue">{format_rate(_rate(behavior['recent_white_or_european_without_caucasian_authors'], behavior['authors']))} of active previous users</div>
        </div>
        <div class="card">
            <h2>No Recent Focal Term</h2>
            <div class="value">{behavior['recent_no_focal_term_authors']:,}</div>
            <div class="subvalue">{format_rate(_rate(behavior['recent_no_focal_term_authors'], behavior['authors']))} of active previous users</div>
        </div>
    </div>

    <h2>Recent Papers by Pre-Recent Author Group</h2>
    <table>
        <thead>
            <tr>
                <th>Recent Paper Group</th>
                <th>Papers</th>
                <th>Caucasian</th>
                <th>White</th>
                <th>European</th>
                <th>White/European Without Caucasian</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>At least one author used Caucasian before {cohort['recent_start_year']}</td>
                <td class="numeric">{recent_pre_c_papers['denominator']:,}</td>
                <td class="numeric">{recent_pre_c_papers['caucasian']:,} ({format_rate(recent_pre_c_papers.get('caucasian_rate'))})</td>
                <td class="numeric">{recent_pre_c_papers['white']:,} ({format_rate(recent_pre_c_papers.get('white_rate'))})</td>
                <td class="numeric">{recent_pre_c_papers['european']:,} ({format_rate(recent_pre_c_papers.get('european_rate'))})</td>
                <td class="numeric">{recent_pre_c_papers['white_or_european_without_caucasian']:,} ({format_rate(recent_pre_c_papers.get('white_or_european_without_caucasian_rate'))})</td>
            </tr>
            <tr>
                <td>Matched pre-recent authors, none with pre-recent Caucasian use</td>
                <td class="numeric">{recent_pre_never_papers['denominator']:,}</td>
                <td class="numeric">{recent_pre_never_papers['caucasian']:,} ({format_rate(recent_pre_never_papers.get('caucasian_rate'))})</td>
                <td class="numeric">{recent_pre_never_papers['white']:,} ({format_rate(recent_pre_never_papers.get('white_rate'))})</td>
                <td class="numeric">{recent_pre_never_papers['european']:,} ({format_rate(recent_pre_never_papers.get('european_rate'))})</td>
                <td class="numeric">{recent_pre_never_papers['white_or_european_without_caucasian']:,} ({format_rate(recent_pre_never_papers.get('white_or_european_without_caucasian_rate'))})</td>
            </tr>
        </tbody>
    </table>

    <h2>Recent Annual Decomposition</h2>
    <table>
        <thead>
            <tr>
                <th>Year</th>
                <th>Processed</th>
                <th>Caucasian Overall</th>
                <th>Matched Author Papers</th>
                <th>Prior-Caucasian-Author Papers</th>
                <th>Caucasian Among Prior-Author Papers</th>
                <th>White/European Without Caucasian Among Prior-Author Papers</th>
            </tr>
        </thead>
        <tbody>
{''.join(annual_rows)}
        </tbody>
    </table>

    <h2>Example Active Previous Users</h2>
    <table>
        <thead>
            <tr>
                <th>Author</th>
                <th>Match</th>
                <th>First Caucasian Year</th>
                <th>Recent Papers</th>
                <th>Recent Caucasian</th>
                <th>Recent White</th>
                <th>Recent European</th>
                <th>Latest Year</th>
            </tr>
        </thead>
        <tbody>
{''.join(example_rows)}
        </tbody>
    </table>
</main>
<script>
const annualScatterData = {json.dumps(annual_scatter_data)};
const caucasianSpanData = {json.dumps(caucasian_span_data)};
const authorUsageScatterData = {json.dumps(author_usage_scatter_data)};

function scatterChart(canvasId, label, points, xTitle, yTitle, color) {{
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    new Chart(canvas.getContext('2d'), {{
        type: 'scatter',
        data: {{
            datasets: [{{
                label,
                data: points,
                backgroundColor: color,
                borderColor: color.replace('0.55', '1'),
                pointRadius: ctx => Math.min(8, 3 + Math.sqrt(ctx.raw.article_count || 1) / 2),
                pointHoverRadius: 8
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: true,
            plugins: {{
                legend: {{ display: false }},
                tooltip: {{
                    callbacks: {{
                        label: context => {{
                            const p = context.raw;
                            if (p.year) return `Year ${{p.year}}: (${{context.parsed.x.toFixed(3)}}, ${{context.parsed.y.toFixed(3)}})`;
                            if (p.display_name) return `${{p.display_name}}: (${{context.parsed.x}}, ${{context.parsed.y}})`;
                            return `(${{context.parsed.x}}, ${{context.parsed.y}})`;
                        }}
                    }}
                }}
            }},
            scales: {{
                x: {{ title: {{ display: true, text: xTitle }}, beginAtZero: true }},
                y: {{ title: {{ display: true, text: yTitle }}, beginAtZero: true }}
            }}
        }}
    }});
}}

scatterChart(
    'caucasianSpanChart',
    'Authors',
    caucasianSpanData.map(p => ({{
        x: p.first_caucasian_year,
        y: p.last_caucasian_year,
        display_name: p.display_name,
        article_count: p.caucasian_count
    }})),
    'First Caucasian year',
    'Last Caucasian year',
    'rgba(158, 42, 43, 0.55)'
);
scatterChart(
    'annualCaucasianWhiteChart',
    'Years',
    annualScatterData.map(p => ({{
        x: p.caucasian_pct,
        y: p.white_pct,
        year: p.year,
        article_count: 1
    }})),
    'Caucasian rate (%)',
    'White rate (%)',
    'rgba(29, 111, 138, 0.55)'
);
scatterChart(
    'annualCaucasianEuropeanChart',
    'Years',
    annualScatterData.map(p => ({{
        x: p.caucasian_pct,
        y: p.european_pct,
        year: p.year,
        article_count: 1
    }})),
    'Caucasian rate (%)',
    'European rate (%)',
    'rgba(59, 127, 59, 0.55)'
);
scatterChart(
    'authorCaucasianWhiteChart',
    'Authors',
    authorUsageScatterData.map(p => ({{
        x: p.caucasian_count,
        y: p.white_count,
        display_name: p.display_name,
        article_count: p.article_count
    }})),
    'Caucasian-positive papers',
    'White-positive papers',
    'rgba(29, 111, 138, 0.45)'
);
scatterChart(
    'authorCaucasianEuropeanChart',
    'Authors',
    authorUsageScatterData.map(p => ({{
        x: p.caucasian_count,
        y: p.european_count,
        display_name: p.display_name,
        article_count: p.article_count
    }})),
    'Caucasian-positive papers',
    'European-positive papers',
    'rgba(59, 127, 59, 0.45)'
);
</script>
</body>
</html>
"""


def write_author_persistence_html(stats: Mapping[str, Any], path: str) -> None:
    with open(path, "w") as output:
        output.write(render_author_persistence_html(stats))


def write_author_persistence_csv(stats: Mapping[str, Any], path: str) -> None:
    fieldnames = [
        "test_id",
        "label",
        "unit",
        "row1_label",
        "row2_label",
        "positive_label",
        "negative_label",
        "row1_positive",
        "row1_negative",
        "row2_positive",
        "row2_negative",
        "row1_rate",
        "row2_rate",
        "risk_difference",
        "odds_ratio_haldane",
        "fisher_exact_p",
        "chi_square",
        "chi_square_p",
    ]
    rows = list(stats["predictive_tests"]) + list(stats["cohort"]["tests"])
    with open(path, "w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def write_author_persistence_annual_csv(stats: Mapping[str, Any], path: str) -> None:
    if stats["annual"]:
        fieldnames = list(stats["annual"][0].keys())
    else:
        fieldnames = ["year"]
    with open(path, "w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in stats["annual"]:
            writer.writerow(row)
