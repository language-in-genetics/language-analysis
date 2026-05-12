#!/usr/bin/env python3
"""Generate a small static report for the full-text AI validation sample."""

from __future__ import annotations

import argparse
import html
import json
import os
from collections import Counter
from datetime import datetime

import psycopg2
import psycopg2.extras


TERMS = [
    ("caucasian", "Caucasian"),
    ("white", "White"),
    ("european", "European"),
    ("other", "Other"),
]

RELATIONSHIP_LABELS = {
    "both": "Both",
    "title_abstract_only": "Title/abstract only",
    "fulltext_only": "Full text only",
    "neither": "Neither",
    "missing_title_abstract": "No title/abstract result",
}

RELATIONSHIP_COLORS = {
    "both": "#2E7D32",
    "title_abstract_only": "#F57C00",
    "fulltext_only": "#1976D2",
    "neither": "#9E9E9E",
    "missing_title_abstract": "#6A1B9A",
}


def boolish(value) -> bool:
    return bool(value)


def term_list(row: dict, prefix: str) -> list[str]:
    terms = []
    for key, label in TERMS:
        if boolish(row.get(f"{prefix}_{key}")):
            terms.append(label)
    return terms


def relationship_key(title_abstract_any: bool | None, fulltext_any: bool) -> str:
    if title_abstract_any is None:
        return "missing_title_abstract"
    if title_abstract_any and fulltext_any:
        return "both"
    if title_abstract_any and not fulltext_any:
        return "title_abstract_only"
    if fulltext_any:
        return "fulltext_only"
    return "neither"


def get_connection():
    return psycopg2.connect("")


def fetch_rows(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            WITH latest_batch AS (
                SELECT slug
                FROM languageingenetics.fulltext_audit_batches
                ORDER BY created_at DESC, slug DESC
                LIMIT 1
            )
            SELECT
                fas.sample_batch,
                fas.article_id,
                fas.doi,
                fas.journal_name,
                fas.pub_year,
                fas.title,
                fas.fulltext_status,
                fas.uploaded_filename,
                fas.ai_analysis_status,
                fas.ai_caucasian AS fulltext_caucasian,
                fas.ai_white AS fulltext_white,
                fas.ai_european AS fulltext_european,
                fas.ai_other AS fulltext_other,
                fas.ai_european_phrase_used,
                fas.ai_other_phrase_used,
                fas.ai_model,
                fas.ai_processed_at,
                f.processed AS title_abstract_processed,
                f.caucasian AS title_abstract_caucasian,
                f.white AS title_abstract_white,
                f.european AS title_abstract_european,
                f.other AS title_abstract_other
            FROM languageingenetics.fulltext_audit_status_view fas
            JOIN latest_batch lb ON lb.slug = fas.sample_batch
            LEFT JOIN languageingenetics.files f
              ON f.article_id = fas.article_id
             AND f.processed = true
            ORDER BY fas.article_id
            """
        )
        return [dict(row) for row in cur.fetchall()]


def build_report_data(rows: list[dict]) -> dict:
    sample_batch = rows[0]["sample_batch"] if rows else ""
    processed_rows = [row for row in rows if row.get("ai_analysis_status") == "processed"]
    paired_rows = [row for row in processed_rows if row.get("title_abstract_processed")]

    relationship_counts = Counter()
    term_relationships = {
        key: Counter({"both": 0, "title_abstract_only": 0, "fulltext_only": 0, "neither": 0})
        for key, _ in TERMS
    }

    fulltext_term_counts = Counter()
    title_abstract_term_counts = Counter()
    table_rows = []

    for row in processed_rows:
        fulltext_terms = term_list(row, "fulltext")
        title_abstract_terms = term_list(row, "title_abstract") if row.get("title_abstract_processed") else []
        fulltext_any = bool(fulltext_terms)
        title_abstract_any = bool(title_abstract_terms) if row.get("title_abstract_processed") else None
        relationship = relationship_key(title_abstract_any, fulltext_any)
        relationship_counts[relationship] += 1

        for term_key, _ in TERMS:
            if row.get(f"fulltext_{term_key}"):
                fulltext_term_counts[term_key] += 1
            if row.get("title_abstract_processed") and row.get(f"title_abstract_{term_key}"):
                title_abstract_term_counts[term_key] += 1

        if row.get("title_abstract_processed"):
            for term_key, _ in TERMS:
                title_abstract_has = boolish(row.get(f"title_abstract_{term_key}"))
                fulltext_has = boolish(row.get(f"fulltext_{term_key}"))
                if title_abstract_has and fulltext_has:
                    term_relationships[term_key]["both"] += 1
                elif title_abstract_has:
                    term_relationships[term_key]["title_abstract_only"] += 1
                elif fulltext_has:
                    term_relationships[term_key]["fulltext_only"] += 1
                else:
                    term_relationships[term_key]["neither"] += 1

        table_rows.append(
            {
                "article_id": row.get("article_id"),
                "journal_name": row.get("journal_name") or "",
                "pub_year": row.get("pub_year") or "",
                "title": row.get("title") or "",
                "title_abstract_terms": title_abstract_terms,
                "fulltext_terms": fulltext_terms,
                "relationship": relationship,
                "ai_model": row.get("ai_model") or "",
                "ai_processed_at": row.get("ai_processed_at").isoformat() if row.get("ai_processed_at") else "",
            }
        )

    return {
        "sample_batch": sample_batch,
        "total": len(rows),
        "uploaded": sum(1 for row in rows if row.get("fulltext_status") == "available"),
        "processed": len(processed_rows),
        "queued": sum(1 for row in rows if row.get("ai_analysis_status") == "queued"),
        "failed": sum(1 for row in rows if row.get("ai_analysis_status") == "failed"),
        "paired": len(paired_rows),
        "fulltext_term_counts": [
            {"term": label, "key": key, "count": fulltext_term_counts[key]}
            for key, label in TERMS
        ],
        "title_abstract_term_counts": [
            {"term": label, "key": key, "count": title_abstract_term_counts[key]}
            for key, label in TERMS
        ],
        "relationship_counts": [
            {
                "key": key,
                "label": RELATIONSHIP_LABELS[key],
                "count": relationship_counts[key],
                "color": RELATIONSHIP_COLORS[key],
            }
            for key in RELATIONSHIP_LABELS
            if relationship_counts[key] or key != "missing_title_abstract"
        ],
        "term_relationships": [
            {
                "term": label,
                "key": key,
                "both": term_relationships[key]["both"],
                "title_abstract_only": term_relationships[key]["title_abstract_only"],
                "fulltext_only": term_relationships[key]["fulltext_only"],
                "neither": term_relationships[key]["neither"],
            }
            for key, label in TERMS
        ],
        "table_rows": table_rows,
    }


def escaped_join(values: list[str]) -> str:
    if not values:
        return "none"
    return ", ".join(html.escape(value) for value in values)


def json_for_script(data: dict) -> str:
    return json.dumps(data, default=str).replace("<", "\\u003c")


def render_html(data: dict) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    processed_pct = (data["processed"] / data["total"] * 100) if data["total"] else 0
    uploaded_pct = (data["uploaded"] / data["total"] * 100) if data["total"] else 0
    rows_html = "\n".join(
        f"""
                    <tr>
                        <td><a href="/cgi-bin/fulltext-status.cgi?batch={html.escape(data['sample_batch'])}&article_id={row['article_id']}">{row['article_id']}</a></td>
                        <td>{html.escape(str(row['pub_year']))}</td>
                        <td>{html.escape(row['journal_name'])}<div class="small">{html.escape(row['title'])}</div></td>
                        <td>{escaped_join(row['title_abstract_terms'])}</td>
                        <td>{escaped_join(row['fulltext_terms'])}</td>
                        <td>{html.escape(RELATIONSHIP_LABELS[row['relationship']])}</td>
                    </tr>
        """
        for row in data["table_rows"]
    )
    if not rows_html:
        rows_html = """
                    <tr><td colspan="6">No full-text articles have been AI processed yet.</td></tr>
        """

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>LIG Full-Text AI Results</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f5f5f5; color: #222; margin: 0; padding: 20px; }}
        .container {{ max-width: 1320px; margin: 0 auto; }}
        h1 {{ margin: 0 0 8px; }}
        h2 {{ margin: 30px 0 14px; color: #444; }}
        a {{ color: #0b63ce; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .small {{ color: #666; font-size: 0.9rem; line-height: 1.35; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 16px; margin: 20px 0 26px; }}
        .card, .chart-container, table {{ background: white; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
        .card {{ padding: 18px; }}
        .card h3 {{ margin: 0 0 10px; color: #666; font-size: 0.85rem; text-transform: uppercase; }}
        .value {{ color: #1976D2; font-size: 2.2rem; font-weight: 700; }}
        .chart-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 18px; }}
        .chart-container {{ padding: 18px; margin-bottom: 18px; }}
        .chart-container h3 {{ margin: 0 0 12px; color: #555; }}
        canvas {{ max-height: 380px; }}
        .method-note {{ background: white; border-left: 4px solid #607D8B; padding: 12px 16px; color: #444; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin: 14px 0 20px; }}
        table {{ width: 100%; border-collapse: collapse; overflow: hidden; }}
        th, td {{ padding: 11px 12px; border-bottom: 1px solid #eee; text-align: left; vertical-align: top; }}
        th {{ background: #f8f8f8; color: #666; font-size: 0.82rem; text-transform: uppercase; }}
        tr:last-child td {{ border-bottom: none; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>LIG Full-Text AI Results</h1>
        <div class="small">Generated {generated_at} · batch <code>{html.escape(data['sample_batch'])}</code> · <a href="/cgi-bin/fulltext-status.cgi?batch={html.escape(data['sample_batch'])}">status table</a> · <a href="/">main dashboard</a></div>

        <div class="grid">
            <div class="card">
                <h3>Sampled Articles</h3>
                <div class="value">{data['total']:,}</div>
                <div class="small">{data['uploaded']:,} uploaded ({uploaded_pct:.1f}%)</div>
            </div>
            <div class="card">
                <h3>AI Processed</h3>
                <div class="value">{data['processed']:,}</div>
                <div class="small">{processed_pct:.1f}% of sampled articles · {data['queued']:,} queued · {data['failed']:,} failed</div>
            </div>
            <div class="card">
                <h3>Paired Comparisons</h3>
                <div class="value">{data['paired']:,}</div>
                <div class="small">Rows with both full-text AI and title/abstract AI results</div>
            </div>
        </div>

        <div class="method-note">
            The relationship charts compare the normal title+abstract AI label with the uploaded full-article AI label for the same sampled article. "Full text only" is the useful operational signal for language found in the uploaded paper but not in the title+abstract result.
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h3>Term Flags Among Processed Full Texts</h3>
                <canvas id="fulltextTermChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>Any Term: Title/Abstract vs Full Text</h3>
                <canvas id="relationshipChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h3>Term-by-Term Relationship</h3>
            <canvas id="termRelationshipChart"></canvas>
        </div>

        <h2>Processed Articles</h2>
        <table>
            <thead>
                <tr>
                    <th>Article</th>
                    <th>Year</th>
                    <th>Journal / Title</th>
                    <th>Title+Abstract</th>
                    <th>Full Text</th>
                    <th>Relationship</th>
                </tr>
            </thead>
            <tbody>
{rows_html}
            </tbody>
        </table>
    </div>

    <script>
        const reportData = {json_for_script(data)};
        const termLabels = reportData.fulltext_term_counts.map(item => item.term);
        new Chart(document.getElementById('fulltextTermChart'), {{
            type: 'bar',
            data: {{
                labels: termLabels,
                datasets: [
                    {{
                        label: 'Title+abstract',
                        data: reportData.title_abstract_term_counts.map(item => item.count),
                        backgroundColor: 'rgba(245, 124, 0, 0.72)'
                    }},
                    {{
                        label: 'Full text',
                        data: reportData.fulltext_term_counts.map(item => item.count),
                        backgroundColor: 'rgba(25, 118, 210, 0.72)'
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ position: 'bottom' }} }},
                scales: {{
                    y: {{ beginAtZero: true, ticks: {{ precision: 0 }} }}
                }}
            }}
        }});

        new Chart(document.getElementById('relationshipChart'), {{
            type: 'doughnut',
            data: {{
                labels: reportData.relationship_counts.map(item => item.label),
                datasets: [{{
                    data: reportData.relationship_counts.map(item => item.count),
                    backgroundColor: reportData.relationship_counts.map(item => item.color)
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ position: 'bottom' }} }}
            }}
        }});

        new Chart(document.getElementById('termRelationshipChart'), {{
            type: 'bar',
            data: {{
                labels: reportData.term_relationships.map(item => item.term),
                datasets: [
                    {{ label: 'Both', data: reportData.term_relationships.map(item => item.both), backgroundColor: '#2E7D32' }},
                    {{ label: 'Title/abstract only', data: reportData.term_relationships.map(item => item.title_abstract_only), backgroundColor: '#F57C00' }},
                    {{ label: 'Full text only', data: reportData.term_relationships.map(item => item.fulltext_only), backgroundColor: '#1976D2' }},
                    {{ label: 'Neither', data: reportData.term_relationships.map(item => item.neither), backgroundColor: '#BDBDBD' }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ position: 'bottom' }} }},
                scales: {{
                    x: {{ stacked: true }},
                    y: {{ stacked: true, beginAtZero: true, ticks: {{ precision: 0 }} }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a full-text AI validation report")
    parser.add_argument("--output", default="../dashboard/fulltext.html", help="HTML output path")
    args = parser.parse_args()

    conn = get_connection()
    try:
        rows = fetch_rows(conn)
    finally:
        conn.close()

    data = build_report_data(rows)
    output_path = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(render_html(data))
    print(f"Full-text report generated at {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
