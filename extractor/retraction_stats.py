"""Retraction-status vocabulary statistics for the LIG Crossref pipeline."""

from __future__ import annotations

import csv
import gzip
import html
import json
import math
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence


OUTCOMES = (
    ("any_race_language", "Any race-language label"),
    ("caucasian", "Caucasian"),
    ("white", "White"),
    ("european", "European"),
    ("other", "Other"),
)

PROCESSED_ARTICLES_SQL = r"""
    WITH processed_files AS MATERIALIZED (
        SELECT
            id,
            work_version_id,
            pub_year,
            COALESCE(caucasian, false) AS caucasian,
            COALESCE(white, false) AS white,
            COALESCE(european, false) AS european,
            COALESCE(other, false) AS other
        FROM languageingenetics.files
        WHERE processed = true
          AND work_version_id IS NOT NULL
        ORDER BY work_version_id
    )
    SELECT
        f.id AS file_id,
        f.work_version_id,
        COALESCE(f.pub_year, v.pub_year) AS pub_year,
        f.caucasian,
        f.white,
        f.european,
        f.other,
        v.work_id,
        v.journal_name,
        v.record_type,
        v.title,
        CASE
            WHEN COALESCE(v.title, '') ~* $$^\s*(retracted|retraction|notice of retraction|expression of concern|editors?[’']?\s+note)$$
                THEN v.raw_json_text
            ELSE NULL
        END AS raw_json_text,
        NULL::text AS normalized_doi
    FROM processed_files f
    JOIN public.crossref_work_versions v ON v.id = f.work_version_id
    JOIN languageingenetics.journals j
        ON j.name = v.journal_name
       AND j.enabled = true
"""

PROCESSED_FILES_SQL = """
    SELECT
        id AS file_id,
        work_id,
        work_version_id,
        pub_year,
        COALESCE(caucasian, false) AS caucasian,
        COALESCE(white, false) AS white,
        COALESCE(european, false) AS european,
        COALESCE(other, false) AS other
    FROM languageingenetics.files
    WHERE processed = true
      AND work_id IS NOT NULL
    ORDER BY work_id
"""

RETRACTED_ARTICLE_TITLE_RE = re.compile(
    r"^\s*retracted(?:\s+article)?\s*[:.-]",
    re.IGNORECASE,
)
RETRACTION_NOTICE_TITLE_RE = re.compile(
    r"^\s*(?:"
    r"retraction\s*(?:note|notice)?(?:\s+(?:to|for)\b|\s*[:.-]|\s*$)|"
    r"notice\s+of\s+retraction\b"
    r")",
    re.IGNORECASE,
)
EXPRESSION_NOTICE_TITLE_RE = re.compile(
    r"^\s*(?:expression\s+of\s+concern|editors?[’']?\s+note\s+to\b)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class RetractionClassification:
    is_retracted_article: bool
    is_retraction_notice: bool
    is_expression_of_concern: bool
    evidence: tuple[str, ...]


def clean_text(value: Any) -> str:
    """Return a compact, markup-free text value."""
    if value is None:
        return ""
    if isinstance(value, list):
        value = " ".join(str(item) for item in value if item is not None)
    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


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


def _normalize_doi(value: Any) -> str:
    return str(value or "").strip().lower()


def _update_items(raw: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    values = raw.get("update-to") or raw.get("update_to") or []
    if isinstance(values, Mapping):
        values = [values]
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        return []
    return [item for item in values if isinstance(item, Mapping)]


def _update_type(item: Mapping[str, Any]) -> str:
    return str(item.get("type") or item.get("label") or "").strip().lower()


def _has_update_type(raw: Mapping[str, Any], needle: str) -> bool:
    return any(needle in _update_type(item) for item in _update_items(raw))


def _retraction_update_targets(raw: Mapping[str, Any]) -> list[str]:
    targets = []
    for item in _update_items(raw):
        if "retraction" not in _update_type(item):
            continue
        doi = _normalize_doi(item.get("DOI") or item.get("doi"))
        if doi:
            targets.append(doi)
    return targets


def classify_retraction_status(row: Mapping[str, Any]) -> RetractionClassification:
    """Classify a processed Crossref work as article, retraction notice, or neither."""
    raw = _parse_raw_json(row.get("raw_json_text"))
    title = clean_text(row.get("title") or raw.get("title"))
    title_lower = title.lower()
    doi = _normalize_doi(row.get("normalized_doi") or raw.get("DOI"))
    evidence: list[str] = []

    notice_by_title = bool(RETRACTION_NOTICE_TITLE_RE.search(title))
    expression_by_title = bool(EXPRESSION_NOTICE_TITLE_RE.search(title))
    retracted_by_title = bool(RETRACTED_ARTICLE_TITLE_RE.search(title))
    retraction_targets = _retraction_update_targets(raw)
    has_retraction_update = bool(retraction_targets)
    has_expression_update = _has_update_type(raw, "expression_of_concern") or _has_update_type(raw, "expression of concern")

    if notice_by_title:
        evidence.append("title_retraction_notice")
    if expression_by_title:
        evidence.append("title_expression_of_concern")
    if retracted_by_title:
        evidence.append("title_retracted_article")
    if has_retraction_update:
        evidence.append("crossref_update_to_retraction")
    if has_expression_update:
        evidence.append("crossref_update_to_expression_of_concern")

    self_retraction_update = bool(doi and doi in retraction_targets)
    third_party_retraction_update = bool(
        retraction_targets and (not doi or any(target != doi for target in retraction_targets))
    )

    # "Retraction Note/Notice" records are update records, not research articles.
    is_retraction_notice = notice_by_title or (third_party_retraction_update and not retracted_by_title)
    is_expression_of_concern = (expression_by_title or has_expression_update) and not is_retraction_notice

    is_retracted_article = False
    if not is_retraction_notice:
        is_retracted_article = retracted_by_title or self_retraction_update

    # Avoid false positives such as "Duane retraction syndrome" or papers about
    # retractions unless Crossref/title metadata marks the record itself.
    if "retraction" in title_lower and not evidence:
        evidence.append("title_mentions_retraction_without_status")

    return RetractionClassification(
        is_retracted_article=is_retracted_article,
        is_retraction_notice=is_retraction_notice,
        is_expression_of_concern=is_expression_of_concern,
        evidence=tuple(evidence),
    )


def load_retraction_status_from_jsonl_gz(path: str) -> dict[str, Any]:
    """Read a focused Crossref JSONL gzip and return DOI-keyed retraction status."""
    status = {
        "source": path,
        "records_scanned": 0,
        "bad_json": 0,
        "gzip_warning": None,
        "retracted_dois": set(),
        "retraction_notice_dois": set(),
        "expression_notice_dois": set(),
        "examples_by_doi": {},
    }

    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as input_file:
            for line in input_file:
                status["records_scanned"] += 1
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    status["bad_json"] += 1
                    continue
                doi = _normalize_doi(item.get("DOI") or item.get("doi"))
                if not doi:
                    continue
                classification = classify_retraction_status({
                    "normalized_doi": doi,
                    "title": item.get("title"),
                    "raw_json_text": item,
                })
                title = clean_text(item.get("title"))
                journal = clean_text(item.get("container-title"))
                pub_year = _crossref_year(item)
                example = {
                    "doi": doi,
                    "journal": journal,
                    "pub_year": pub_year,
                    "title": title,
                    "evidence": list(classification.evidence),
                }
                if classification.is_retraction_notice:
                    status["retraction_notice_dois"].add(doi)
                    continue
                if classification.is_expression_of_concern and not classification.is_retracted_article:
                    status["expression_notice_dois"].add(doi)
                    continue
                if classification.is_retracted_article:
                    status["retracted_dois"].add(doi)
                    status["examples_by_doi"][doi] = example
    except (EOFError, gzip.BadGzipFile) as exc:
        status["gzip_warning"] = f"{type(exc).__name__}: {exc}"

    return status


def load_retraction_status_from_sqlite(path: str, category: str = "focused") -> dict[str, Any]:
    """Read a focused Crossref SQLite stage and return DOI-keyed retraction status."""
    status = {
        "source": path,
        "source_category": category,
        "records_scanned": 0,
        "bad_json": 0,
        "sqlite_warning": None,
        "retracted_dois": set(),
        "retraction_notice_dois": set(),
        "expression_notice_dois": set(),
        "examples_by_doi": {},
    }

    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
            for (raw_json_text,) in conn.execute(
                """
                SELECT raw_json_text
                FROM import_records
                WHERE category = ?
                ORDER BY id
                """,
                (category,),
            ):
                status["records_scanned"] += 1
                try:
                    item = json.loads(raw_json_text)
                except json.JSONDecodeError:
                    status["bad_json"] += 1
                    continue
                _add_retraction_status_item(status, item)
    except sqlite3.Error as exc:
        status["sqlite_warning"] = f"{type(exc).__name__}: {exc}"

    return status


def _add_retraction_status_item(status: dict[str, Any], item: Mapping[str, Any]) -> None:
    doi = _normalize_doi(item.get("DOI") or item.get("doi"))
    if not doi:
        return
    classification = classify_retraction_status({
        "normalized_doi": doi,
        "title": item.get("title"),
        "raw_json_text": item,
    })
    title = clean_text(item.get("title"))
    journal = clean_text(item.get("container-title"))
    pub_year = _crossref_year(item)
    example = {
        "doi": doi,
        "journal": journal,
        "pub_year": pub_year,
        "title": title,
        "evidence": list(classification.evidence),
    }
    if classification.is_retraction_notice:
        status["retraction_notice_dois"].add(doi)
        return
    if classification.is_expression_of_concern and not classification.is_retracted_article:
        status["expression_notice_dois"].add(doi)
        return
    if classification.is_retracted_article:
        status["retracted_dois"].add(doi)
        status["examples_by_doi"][doi] = example


def _crossref_year(item: Mapping[str, Any]) -> int | None:
    for key in ("published-print", "published-online", "published", "issued", "created"):
        value = item.get(key)
        if not isinstance(value, Mapping):
            continue
        date_parts = value.get("date-parts") or []
        if date_parts and date_parts[0]:
            try:
                return int(date_parts[0][0])
            except (TypeError, ValueError):
                return None
    return None


def resolve_status_work_ids(cursor: Any, status: Mapping[str, Any]) -> dict[str, Any]:
    """Map DOI-keyed status sets onto local crossref_works IDs."""
    all_dois = sorted(
        set(status["retracted_dois"])
        | set(status["retraction_notice_dois"])
        | set(status["expression_notice_dois"])
    )
    doi_to_work_id: dict[str, int] = {}
    if all_dois:
        cursor.execute(
            """
            SELECT id, normalized_doi
            FROM public.crossref_works
            WHERE normalized_doi = ANY(%s)
            """,
            [all_dois],
        )
        doi_to_work_id = {
            row["normalized_doi"]: row["id"]
            for row in cursor.fetchall()
            if row.get("normalized_doi") is not None
        }

    examples_by_work_id = {}
    for doi, example in status["examples_by_doi"].items():
        work_id = doi_to_work_id.get(doi)
        if work_id is None:
            continue
        enriched = dict(example)
        enriched["work_id"] = work_id
        examples_by_work_id[work_id] = enriched

    return {
        "retracted_work_ids": {doi_to_work_id[doi] for doi in status["retracted_dois"] if doi in doi_to_work_id},
        "retraction_notice_work_ids": {
            doi_to_work_id[doi] for doi in status["retraction_notice_dois"] if doi in doi_to_work_id
        },
        "expression_notice_work_ids": {
            doi_to_work_id[doi] for doi in status["expression_notice_dois"] if doi in doi_to_work_id
        },
        "examples_by_work_id": examples_by_work_id,
        "source": status["source"],
        "records_scanned": status["records_scanned"],
        "bad_json": status["bad_json"],
        "gzip_warning": status["gzip_warning"],
    }


def _log_comb(n: int, k: int) -> float:
    if k < 0 or k > n:
        return float("-inf")
    return math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)


def _hypergeom_probability(a: int, row1: int, col1: int, n: int) -> float:
    return math.exp(_log_comb(col1, a) + _log_comb(n - col1, row1 - a) - _log_comb(n, row1))


def fisher_exact_two_sided(a: int, b: int, c: int, d: int) -> float | None:
    """Two-sided Fisher exact p-value for a 2x2 table.

    Table layout:
        [[a, b],
         [c, d]]
    """
    if min(a, b, c, d) < 0:
        raise ValueError("contingency counts must be non-negative")
    row1 = a + b
    row2 = c + d
    col1 = a + c
    n = row1 + row2
    if n == 0 or row1 == 0 or row2 == 0:
        return None

    observed = _hypergeom_probability(a, row1, col1, n)
    lower = max(0, row1 - (n - col1))
    upper = min(row1, col1)
    total = 0.0
    eps = observed * 1e-12 + 1e-15
    for possible_a in range(lower, upper + 1):
        probability = _hypergeom_probability(possible_a, row1, col1, n)
        if probability <= observed + eps:
            total += probability
    return min(1.0, total)


def chi_square_test_2x2(a: int, b: int, c: int, d: int) -> tuple[float | None, float | None]:
    """Pearson chi-square statistic and df=1 p-value for a 2x2 table."""
    n = a + b + c + d
    row1 = a + b
    row2 = c + d
    col1 = a + c
    col2 = b + d
    denominator = row1 * row2 * col1 * col2
    if n == 0 or denominator == 0:
        return None, None
    statistic = n * ((a * d - b * c) ** 2) / denominator
    p_value = math.erfc(math.sqrt(statistic / 2))
    return statistic, p_value


def _rate(part: int, whole: int) -> float | None:
    return part / whole if whole else None


def _odds_ratio_haldane(a: int, b: int, c: int, d: int) -> float:
    return ((a + 0.5) * (d + 0.5)) / ((b + 0.5) * (c + 0.5))


def _risk_difference(a: int, b: int, c: int, d: int) -> float | None:
    retracted_rate = _rate(a, a + b)
    non_retracted_rate = _rate(c, c + d)
    if retracted_rate is None or non_retracted_rate is None:
        return None
    return retracted_rate - non_retracted_rate


def build_retraction_statistics(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Build retracted-vs-non-retracted vocabulary tests from processed rows."""
    rows = list(rows)
    tested_rows = []
    retracted_examples = []
    excluded_retraction_notices = 0
    excluded_expression_of_concern = 0
    unknown_status_mentions = 0

    for row in rows:
        classification = classify_retraction_status(row)
        title = clean_text(row.get("title"))

        if classification.is_retraction_notice:
            excluded_retraction_notices += 1
            continue
        if classification.is_expression_of_concern and not classification.is_retracted_article:
            excluded_expression_of_concern += 1
            continue
        if "title_mentions_retraction_without_status" in classification.evidence:
            unknown_status_mentions += 1

        enriched = dict(row)
        enriched["is_retracted_article"] = classification.is_retracted_article
        enriched["retraction_evidence"] = list(classification.evidence)
        enriched["any_race_language"] = any(
            bool(enriched.get(key)) for key in ("caucasian", "white", "european", "other")
        )
        tested_rows.append(enriched)

        if classification.is_retracted_article and len(retracted_examples) < 25:
            retracted_examples.append({
                "doi": enriched.get("normalized_doi"),
                "work_id": enriched.get("work_id"),
                "work_version_id": enriched.get("work_version_id"),
                "journal": enriched.get("journal_name"),
                "pub_year": enriched.get("pub_year"),
                "title": title,
                "evidence": list(classification.evidence),
                "any_race_language": enriched["any_race_language"],
                "caucasian": bool(enriched.get("caucasian")),
                "white": bool(enriched.get("white")),
                "european": bool(enriched.get("european")),
                "other": bool(enriched.get("other")),
            })

    retracted_count = sum(1 for row in tested_rows if row["is_retracted_article"])
    non_retracted_count = len(tested_rows) - retracted_count
    tests = []

    for key, label in OUTCOMES:
        a = sum(1 for row in tested_rows if row["is_retracted_article"] and bool(row.get(key)))
        b = retracted_count - a
        c = sum(1 for row in tested_rows if not row["is_retracted_article"] and bool(row.get(key)))
        d = non_retracted_count - c
        chi_square, chi_square_p = chi_square_test_2x2(a, b, c, d)
        tests.append({
            "outcome": key,
            "label": label,
            "retracted_with_term": a,
            "retracted_without_term": b,
            "non_retracted_with_term": c,
            "non_retracted_without_term": d,
            "retracted_rate": _rate(a, a + b),
            "non_retracted_rate": _rate(c, c + d),
            "risk_difference": _risk_difference(a, b, c, d),
            "odds_ratio_haldane": _odds_ratio_haldane(a, b, c, d) if (a + b and c + d) else None,
            "fisher_exact_p": fisher_exact_two_sided(a, b, c, d),
            "chi_square": chi_square,
            "chi_square_p": chi_square_p,
        })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "population": {
            "processed_focused_articles": len(rows),
            "eligible_articles": len(tested_rows),
            "retracted_articles": retracted_count,
            "non_retracted_articles": non_retracted_count,
            "excluded_retraction_notices": excluded_retraction_notices,
            "excluded_expression_of_concern": excluded_expression_of_concern,
            "unknown_status_mentions": unknown_status_mentions,
        },
        "tests": tests,
        "retracted_examples": retracted_examples,
        "method": {
            "primary_test": "Fisher exact test, two-sided",
            "secondary_test": "Pearson chi-square test, df=1",
            "case_definition": (
                "Research article records whose title starts with Retracted/RETRACTED ARTICLE "
                "or whose retrieved Crossref update-to metadata self-identifies a retraction."
            ),
            "excluded_records": (
                "Retraction notes/notices and expression-of-concern update records are excluded "
                "from both case and control groups."
            ),
            "performance_note": (
                "Raw Crossref JSON is fetched only for likely retraction-status title candidates; "
                "control rows use extracted current-version columns."
            ),
        },
    }


def build_retraction_statistics_from_work_ids(
    rows: Iterable[Mapping[str, Any]],
    status_work_ids: Mapping[str, Any],
) -> dict[str, Any]:
    """Build vocabulary tests using a precomputed set of retracted work IDs."""
    rows = list(rows)
    retracted_work_ids = set(status_work_ids.get("retracted_work_ids", set()))
    notice_work_ids = set(status_work_ids.get("retraction_notice_work_ids", set()))
    expression_work_ids = set(status_work_ids.get("expression_notice_work_ids", set()))
    examples_by_work_id = status_work_ids.get("examples_by_work_id", {})

    tested_rows = []
    for row in rows:
        work_id = row.get("work_id")
        if work_id in notice_work_ids or work_id in expression_work_ids:
            continue
        enriched = dict(row)
        enriched["is_retracted_article"] = work_id in retracted_work_ids
        enriched["any_race_language"] = any(
            bool(enriched.get(key)) for key in ("caucasian", "white", "european", "other")
        )
        tested_rows.append(enriched)

    processed_work_ids = {row.get("work_id") for row in rows}
    retracted_examples = []
    for work_id, example in examples_by_work_id.items():
        if work_id not in processed_work_ids:
            continue
        matching_rows = [row for row in tested_rows if row.get("work_id") == work_id]
        if not matching_rows:
            continue
        row = matching_rows[0]
        enriched_example = dict(example)
        enriched_example.update({
            "work_id": work_id,
            "work_version_id": row.get("work_version_id"),
            "any_race_language": row.get("any_race_language", False),
            "caucasian": bool(row.get("caucasian")),
            "white": bool(row.get("white")),
            "european": bool(row.get("european")),
            "other": bool(row.get("other")),
        })
        retracted_examples.append(enriched_example)
        if len(retracted_examples) >= 25:
            break

    retracted_count = sum(1 for row in tested_rows if row["is_retracted_article"])
    non_retracted_count = len(tested_rows) - retracted_count
    tests = []

    for key, label in OUTCOMES:
        a = sum(1 for row in tested_rows if row["is_retracted_article"] and bool(row.get(key)))
        b = retracted_count - a
        c = sum(1 for row in tested_rows if not row["is_retracted_article"] and bool(row.get(key)))
        d = non_retracted_count - c
        chi_square, chi_square_p = chi_square_test_2x2(a, b, c, d)
        tests.append({
            "outcome": key,
            "label": label,
            "retracted_with_term": a,
            "retracted_without_term": b,
            "non_retracted_with_term": c,
            "non_retracted_without_term": d,
            "retracted_rate": _rate(a, a + b),
            "non_retracted_rate": _rate(c, c + d),
            "risk_difference": _risk_difference(a, b, c, d),
            "odds_ratio_haldane": _odds_ratio_haldane(a, b, c, d) if (a + b and c + d) else None,
            "fisher_exact_p": fisher_exact_two_sided(a, b, c, d),
            "chi_square": chi_square,
            "chi_square_p": chi_square_p,
        })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "population": {
            "processed_focused_articles": len(rows),
            "eligible_articles": len(tested_rows),
            "retracted_articles": retracted_count,
            "non_retracted_articles": non_retracted_count,
            "excluded_retraction_notices": len(processed_work_ids & notice_work_ids),
            "excluded_expression_of_concern": len(processed_work_ids & expression_work_ids),
            "unknown_status_mentions": 0,
        },
        "tests": tests,
        "retracted_examples": retracted_examples,
        "method": {
            "primary_test": "Fisher exact test, two-sided",
            "secondary_test": "Pearson chi-square test, df=1",
            "case_definition": (
                "Research article records marked as retracted in the focused Crossref JSONL source."
            ),
            "excluded_records": (
                "Retraction notes/notices and expression-of-concern update records are excluded "
                "from both case and control groups when their DOI maps to a processed work."
            ),
            "status_source": status_work_ids.get("source"),
            "status_records_scanned": status_work_ids.get("records_scanned"),
            "status_bad_json": status_work_ids.get("bad_json"),
            "status_gzip_warning": status_work_ids.get("gzip_warning"),
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


def render_stats_html(stats: Mapping[str, Any], title: str = "Retraction Status and Race-Language Tests") -> str:
    """Render a standalone HTML report for the retraction vocabulary tests."""
    population = stats["population"]
    generated_at = html.escape(str(stats.get("generated_at", "")))
    method = stats.get("method", {})

    test_rows = []
    for test in stats["tests"]:
        retracted_total = test["retracted_with_term"] + test["retracted_without_term"]
        control_total = test["non_retracted_with_term"] + test["non_retracted_without_term"]
        test_rows.append(f"""
                <tr>
                    <td>{html.escape(str(test["label"]))}</td>
                    <td class="numeric">{test["retracted_with_term"]:,} / {retracted_total:,}</td>
                    <td class="numeric">{format_rate(test["retracted_rate"])}</td>
                    <td class="numeric">{test["non_retracted_with_term"]:,} / {control_total:,}</td>
                    <td class="numeric">{format_rate(test["non_retracted_rate"])}</td>
                    <td class="numeric">{_format_risk_difference(test["risk_difference"])}</td>
                    <td class="numeric">{_format_odds_ratio(test["odds_ratio_haldane"])}</td>
                    <td class="numeric">{format_p_value(test["fisher_exact_p"])}</td>
                    <td class="numeric">{format_p_value(test["chi_square_p"])}</td>
                </tr>""")

    example_rows = []
    for item in stats.get("retracted_examples", []):
        example_rows.append(f"""
                <tr>
                    <td>{html.escape(str(item.get("pub_year") or ""))}</td>
                    <td>{html.escape(str(item.get("journal") or ""))}</td>
                    <td>{html.escape(str(item.get("doi") or item.get("work_version_id") or ""))}</td>
                    <td>{html.escape(str(item.get("title") or ""))}</td>
                    <td>{html.escape(", ".join(item.get("evidence") or []))}</td>
                    <td>{"Yes" if item.get("any_race_language") else "No"}</td>
                </tr>""")
    if not example_rows:
        example_rows.append("""
                <tr><td colspan="6">No retracted article records have been processed yet.</td></tr>""")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{html.escape(title)}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            margin: 0;
            padding: 2rem;
            background: #f6f7fb;
            color: #1f2933;
        }}
        main {{
            max-width: 1200px;
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
        }}
        a {{
            color: #0967d2;
        }}
    </style>
</head>
<body>
<main>
    <p><a href="index.html">Back to dashboard</a></p>
    <h1>{html.escape(title)}</h1>
    <p class="meta">Generated: {generated_at}</p>
    <div class="grid">
        <div class="card">
            <h2>Eligible Articles</h2>
            <div class="value">{population["eligible_articles"]:,}</div>
            <div class="subvalue">{population["processed_focused_articles"]:,} processed focused articles checked</div>
        </div>
        <div class="card">
            <h2>Retracted Articles</h2>
            <div class="value">{population["retracted_articles"]:,}</div>
            <div class="subvalue">Research article records flagged as retracted</div>
        </div>
        <div class="card">
            <h2>Excluded Notices</h2>
            <div class="value">{population["excluded_retraction_notices"]:,}</div>
            <div class="subvalue">Retraction notes/notices excluded from tests</div>
        </div>
    </div>
    <p class="method-note">
        Primary test: {html.escape(str(method.get("primary_test", "Fisher exact test, two-sided")))}.
        Secondary check: {html.escape(str(method.get("secondary_test", "Pearson chi-square test, df=1")))}.
        Case definition: {html.escape(str(method.get("case_definition", "")))}.
        Exclusions: {html.escape(str(method.get("excluded_records", "")))}.
        Machine-readable outputs: <a href="retraction_statistics.json">JSON</a> and
        <a href="retraction_statistics.csv">CSV</a>.
    </p>
    <h2>Vocabulary Tests</h2>
    <table>
        <thead>
            <tr>
                <th>Outcome</th>
                <th>Retracted With Term</th>
                <th>Retracted Rate</th>
                <th>Control With Term</th>
                <th>Control Rate</th>
                <th>Risk Difference</th>
                <th>Odds Ratio</th>
                <th>Fisher p</th>
                <th>Chi-square p</th>
            </tr>
        </thead>
        <tbody>
{''.join(test_rows)}
        </tbody>
    </table>
    <h2>Detected Retracted Article Records</h2>
    <table>
        <thead>
            <tr>
                <th>Year</th>
                <th>Journal</th>
                <th>DOI / Work Version</th>
                <th>Title</th>
                <th>Evidence</th>
                <th>Race Language?</th>
            </tr>
        </thead>
        <tbody>
{''.join(example_rows)}
        </tbody>
    </table>
</main>
</body>
</html>
"""


def write_stats_html(stats: Mapping[str, Any], path: str) -> None:
    with open(path, "w") as output:
        output.write(render_stats_html(stats))


def write_stats_csv(stats: Mapping[str, Any], path: str) -> None:
    fieldnames = [
        "outcome",
        "label",
        "retracted_with_term",
        "retracted_without_term",
        "non_retracted_with_term",
        "non_retracted_without_term",
        "retracted_rate",
        "non_retracted_rate",
        "risk_difference",
        "odds_ratio_haldane",
        "fisher_exact_p",
        "chi_square",
        "chi_square_p",
    ]
    with open(path, "w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in stats["tests"]:
            writer.writerow({field: row.get(field) for field in fieldnames})
