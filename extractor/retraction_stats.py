"""Retraction-status vocabulary statistics for the LIG Crossref pipeline."""

from __future__ import annotations

import csv
import html
import json
import math
import re
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
