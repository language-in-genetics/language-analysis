#!/usr/bin/env python3

import argparse
import html
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
import psycopg2.extras

parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", default="dashboard", help="Output directory for static files")
parser.add_argument("--explain-queries", action="store_true", help="Run EXPLAIN on all queries and log to file")
parser.add_argument("--explain-log", default="query_explains.log", help="Log file for EXPLAIN output")
args = parser.parse_args()

# Track script runtime
start_time = time.time()

# Create output directory
os.makedirs(args.output_dir, exist_ok=True)

# Database connection using environment variables (PGDATABASE, PGHOST, etc.)
conn = psycopg2.connect("")
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def json_default(obj):
    """Convert Decimal instances returned by psycopg2 into JSON-serializable numbers."""
    if isinstance(obj, Decimal):
        return int(obj) if obj == obj.to_integral_value() else float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


TAG_RE = re.compile(r"<[^>]+>")

WORD_PATTERNS = {
    'caucasian': re.compile(r"\bcaucasian(s)?\b", re.IGNORECASE),
    'white': re.compile(r"\bwhite\b", re.IGNORECASE),
    'european': re.compile(r"\beuropean(s)?\b", re.IGNORECASE),
}


def normalize_text(text):
    """Normalize text for loose matching of terminology."""
    if not text:
        return ""
    cleaned = html.unescape(TAG_RE.sub(" ", text))
    cleaned = cleaned.replace("-", " ")
    cleaned = cleaned.replace("/", " ")
    cleaned = cleaned.replace("_", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip().lower()


def lower_with_html(text):
    """Lowercase text while stripping simple markup."""
    if not text:
        return ""
    return html.unescape(TAG_RE.sub(" ", text)).lower()


def split_phrases(phrase_text):
    """Split phrases that may contain multiple comma/semicolon separated entries."""
    if not phrase_text:
        return []
    parts = [part.strip() for part in re.split(r"[;,/]+", phrase_text) if part.strip()]
    return parts or [phrase_text.strip()]


def contains_phrase(text_norm, text_lower, phrase):
    """Check whether the provided phrase appears in the normalized or raw text."""
    if not phrase:
        return False
    cleaned = phrase.strip().strip('\"\'\u201c\u201d\u2018\u2019')
    if not cleaned:
        return False
    normalized_phrase = normalize_text(cleaned)
    if normalized_phrase and normalized_phrase in text_norm:
        return True
    return cleaned.lower() in text_lower

# Helper function to execute queries with optional EXPLAIN logging
def execute_query(sql, params=None):
    """Execute a query, optionally logging EXPLAIN output"""
    if args.explain_queries:
        # Use a separate connection for EXPLAIN to avoid transaction conflicts
        explain_conn = psycopg2.connect("")
        explain_cursor = explain_conn.cursor()
        try:
            explain_cursor.execute("SET search_path TO languageingenetics, public")
            if params:
                explain_cursor.execute("EXPLAIN (ANALYZE, BUFFERS, VERBOSE) " + sql, params)
            else:
                explain_cursor.execute("EXPLAIN (ANALYZE, BUFFERS, VERBOSE) " + sql)
            explain_output = "\n".join(row[0] for row in explain_cursor.fetchall())

            with open(args.explain_log, 'a') as f:
                f.write(f"\n{'='*80}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Query:\n{sql}\n")
                if params:
                    f.write(f"Parameters: {params}\n")
                f.write(f"\nEXPLAIN output:\n{explain_output}\n")
        except psycopg2.Error as e:
            # Log the error but don't fail the entire script
            with open(args.explain_log, 'a') as f:
                f.write(f"\n{'='*80}\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Query:\n{sql}\n")
                f.write(f"\nEXPLAIN error: {e}\n")
        finally:
            explain_cursor.close()
            explain_conn.close()

    # Execute the actual query
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    return cursor

# Set search path
cursor.execute("SET search_path TO languageingenetics, public")

# Initialize explain log if needed
if args.explain_queries:
    with open(args.explain_log, 'w') as f:
        f.write(f"Query Explanation Log - Generated {datetime.now().isoformat()}\n")
        f.write(f"{'='*80}\n")

print("Collecting data...")

# Query journals_mv first to get article counts efficiently
# If MV doesn't exist, we'll use a slower method
try:
    execute_query("""
        SELECT
            journal_name,
            article_count,
            earliest_year,
            latest_year,
            articles_with_abstract,
            abstract_percentage,
            total_citations,
            avg_citations_per_article
        FROM public.journals_mv
        WHERE journal_name ILIKE '%genetic%'
        ORDER BY article_count DESC, journal_name
    """)
    journals_mv_data = {row['journal_name']: row for row in cursor.fetchall()}
    using_mv = True
    print(f"Using journals_mv with {len(journals_mv_data)} journals", file=sys.stderr)
except psycopg2.Error:
    conn.rollback()
    using_mv = False
    journals_mv_data = {}
    print("Warning: journals_mv not available, will query raw_text_data", file=sys.stderr)

# Get enabled journals first - we'll calculate total from journal stats
execute_query("SELECT name FROM languageingenetics.journals WHERE enabled = true ORDER BY name")
enabled_journals = [row['name'] for row in cursor]

execute_query("SELECT COUNT(*) FROM languageingenetics.files WHERE processed = true")
processed_articles = cursor.fetchone()['count']

# Calculate completion projection
execute_query("""
    SELECT MIN(when_processed) as earliest_processed
    FROM languageingenetics.files
    WHERE processed = true AND when_processed IS NOT NULL
""")
earliest = cursor.fetchone()['earliest_processed']

# Token usage data
execute_query("""
    SELECT
        COALESCE(SUM(prompt_tokens), 0) as prompt_tokens,
        COALESCE(SUM(completion_tokens), 0) as completion_tokens
    FROM languageingenetics.files
    WHERE processed = true
""")
row = cursor.fetchone()
all_time_prompt = row['prompt_tokens']
all_time_completion = row['completion_tokens']

last_24h = datetime.now() - timedelta(hours=24)
execute_query("""
    SELECT
        COALESCE(SUM(prompt_tokens), 0) as prompt_tokens,
        COALESCE(SUM(completion_tokens), 0) as completion_tokens
    FROM languageingenetics.files
    WHERE processed = true AND when_processed >= %s
""", [last_24h])
row = cursor.fetchone()
last24h_prompt = row['prompt_tokens']
last24h_completion = row['completion_tokens']

token_data = {
    'all_time': {
        'prompt_tokens': all_time_prompt,
        'completion_tokens': all_time_completion,
        'total_tokens': all_time_prompt + all_time_completion
    },
    'last_24h': {
        'prompt_tokens': last24h_prompt,
        'completion_tokens': last24h_completion,
        'total_tokens': last24h_prompt + last24h_completion
    }
}

# Calculate batch waiting time in last 24 hours
execute_query("""
    SELECT
        b.id,
        b.when_sent,
        MIN(bp.when_checked) as first_progress,
        MAX(bp.when_checked) as last_progress
    FROM languageingenetics.batches b
    LEFT JOIN languageingenetics.batchprogress bp ON b.id = bp.batch_id
    WHERE b.when_sent >= %s
    GROUP BY b.id, b.when_sent
    ORDER BY b.when_sent
""", [last_24h])

batch_rows = cursor.fetchall()
total_waiting_seconds = 0

for batch in batch_rows:
    if batch['first_progress'] and batch['when_sent']:
        # Time from when batch was sent until first progress update
        wait_time = (batch['first_progress'] - batch['when_sent']).total_seconds()
        if wait_time > 0:
            total_waiting_seconds += wait_time

waiting_hours = total_waiting_seconds / 3600.0
batch_utilization = ((24 - waiting_hours) / 24 * 100) if waiting_hours < 24 else 0

# Journal statistics - use MV data if available to avoid slow queries
journal_stats = []
for journal in enabled_journals:
    # Try to get total from journals_mv first
    if using_mv and journal in journals_mv_data:
        total = journals_mv_data[journal]['article_count']
    else:
        # Fall back to querying raw_text_data (slow!)
        execute_query("""
            SELECT COUNT(*) as total
            FROM public.raw_text_data
            WHERE (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title' @> %s::jsonb)
        """, [json.dumps([journal], default=json_default)])
        total = cursor.fetchone()['total']

    if total > 0:
        # Get processed count
        execute_query("""
            SELECT COUNT(*) as processed
            FROM languageingenetics.files f
            JOIN public.raw_text_data r ON f.article_id = r.id
            WHERE f.processed = true
            AND (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title' @> %s::jsonb)
        """, [json.dumps([journal], default=json_default)])
        processed = cursor.fetchone()['processed']
    else:
        processed = 0

    journal_stats.append({
        'journal': journal,
        'total': total,
        'processed': processed
    })

# Calculate total articles from journal stats
total_articles = sum(j['total'] for j in journal_stats)

# Calculate progress data and completion projection
progress_data = {
    'total_articles': total_articles,
    'processed_articles': processed_articles,
    'unprocessed_articles': total_articles - processed_articles,
    'processing_percentage': (processed_articles / total_articles * 100) if total_articles > 0 else 0
}

if earliest and processed_articles > 0 and total_articles > processed_articles:
    time_elapsed = (datetime.now() - earliest).total_seconds()
    articles_per_second = processed_articles / time_elapsed
    remaining_articles = total_articles - processed_articles
    seconds_remaining = remaining_articles / articles_per_second
    completion_date = datetime.now() + timedelta(seconds=seconds_remaining)
else:
    completion_date = None

# Results by year with terminology breakdown
execute_query("""
    SELECT
        pub_year AS year,
        COUNT(*) AS total_count,
        COUNT(*) FILTER (WHERE caucasian = true) AS caucasian_count,
        COUNT(*) FILTER (WHERE white = true) AS white_count,
        COUNT(*) FILTER (WHERE european = true) AS european_count,
        COUNT(*) FILTER (WHERE other = true) AS other_count,
        COUNT(*) FILTER (WHERE (caucasian = true OR white = true OR european = true OR other = true)) AS any_count
    FROM languageingenetics.files
    WHERE processed = true
    AND pub_year IS NOT NULL
    GROUP BY pub_year
    ORDER BY pub_year
""")
term_year_rows = cursor.fetchall()

term_year_data = []
by_year = []


def pct(part, whole):
    return round((part / whole) * 100, 4) if whole else 0.0


for row in term_year_rows:
    total = row['total_count'] or 0
    caucasian_count = row['caucasian_count'] or 0
    white_count = row['white_count'] or 0
    european_count = row['european_count'] or 0
    other_count = row['other_count'] or 0
    any_count = row['any_count'] or 0

    term_year_data.append({
        'year': row['year'],
        'total_count': total,
        'caucasian_count': caucasian_count,
        'white_count': white_count,
        'european_count': european_count,
        'other_count': other_count,
        'any_count': any_count,
        'caucasian_pct': pct(caucasian_count, total),
        'white_pct': pct(white_count, total),
        'european_pct': pct(european_count, total),
        'other_pct': pct(other_count, total),
        'any_pct': pct(any_count, total)
    })

    by_year.append({
        'year': row['year'],
        'count': any_count,
        'total_articles': total
    })

# Results by journal and year
execute_query("""
    SELECT
        (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title'->0) as journal,
        f.pub_year as year,
        COUNT(*) as count
    FROM languageingenetics.files f
    JOIN public.raw_text_data r ON f.article_id = r.id
    WHERE f.processed = true
    AND (f.caucasian = true OR f.white = true OR f.european = true OR f.other = true)
    AND f.pub_year IS NOT NULL
    GROUP BY journal, year
    ORDER BY journal, year
""")
by_journal_year_final = []
for row in cursor.fetchall():
    if row['journal']:
        journal_name = row['journal'].strip('"') if isinstance(row['journal'], str) else row['journal']
        by_journal_year_final.append({
            'journal': journal_name,
            'year': row['year'],
            'count': row['count']
        })

# Article-level extraction for title vs abstract analysis
execute_query("""
    SELECT
        f.article_id,
        f.pub_year,
        f.has_abstract,
        f.caucasian,
        f.white,
        f.european,
        f.other,
        f.european_phrase_used,
        f.other_phrase_used,
        (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title'->>0) as journal,
        (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'title'->>0) as title,
        (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'abstract'->>0) as abstract
    FROM languageingenetics.files f
    JOIN public.raw_text_data r ON f.article_id = r.id
    WHERE f.processed = true
""")

article_rows = cursor.fetchall()

journal_term_totals = defaultdict(lambda: {'title': 0, 'abstract': 0, 'both': 0, 'total': 0})
year_term_totals = defaultdict(lambda: {'title': 0, 'abstract': 0, 'both': 0, 'total': 0})

for row in article_rows:
    has_any_term = any([
        row['caucasian'],
        row['white'],
        row['european'],
        row['other'],
    ])

    if not has_any_term:
        continue

    journal_value = row['journal']
    if isinstance(journal_value, str):
        journal_value = journal_value.strip().strip('"')
    journal_name = journal_value if journal_value else None

    year_value = row['pub_year']

    title_lower = lower_with_html(row['title'])
    abstract_lower = lower_with_html(row['abstract'])
    title_norm = normalize_text(row['title'])
    abstract_norm = normalize_text(row['abstract'])

    title_has = False
    abstract_has = False

    if row['caucasian']:
        if WORD_PATTERNS['caucasian'].search(title_lower):
            title_has = True
        if WORD_PATTERNS['caucasian'].search(abstract_lower):
            abstract_has = True

    if row['white']:
        if WORD_PATTERNS['white'].search(title_lower):
            title_has = True
        if WORD_PATTERNS['white'].search(abstract_lower):
            abstract_has = True

    if row['european']:
        european_phrases = split_phrases(row['european_phrase_used'])
        if not european_phrases:
            european_phrases = ['european']
        if any(contains_phrase(title_norm, title_lower, phrase) for phrase in european_phrases) or WORD_PATTERNS['european'].search(title_lower):
            title_has = True
        if any(contains_phrase(abstract_norm, abstract_lower, phrase) for phrase in european_phrases) or WORD_PATTERNS['european'].search(abstract_lower):
            abstract_has = True

    if row['other']:
        other_phrases = split_phrases(row['other_phrase_used'])
        for phrase in other_phrases:
            if contains_phrase(title_norm, title_lower, phrase):
                title_has = True
                break
        for phrase in other_phrases:
            if contains_phrase(abstract_norm, abstract_lower, phrase):
                abstract_has = True
                break

    journal_counts = None
    if journal_name:
        journal_counts = journal_term_totals[journal_name]
        journal_counts['total'] += 1

    year_counts = None
    if year_value is not None:
        year_counts = year_term_totals[year_value]
        year_counts['total'] += 1

    if title_has:
        if journal_counts is not None:
            journal_counts['title'] += 1
        if year_counts is not None:
            year_counts['title'] += 1

    if abstract_has:
        if journal_counts is not None:
            journal_counts['abstract'] += 1
        if year_counts is not None:
            year_counts['abstract'] += 1

    if title_has and abstract_has:
        if journal_counts is not None:
            journal_counts['both'] += 1
        if year_counts is not None:
            year_counts['both'] += 1

journal_scatter_data = []
for journal, counts in journal_term_totals.items():
    if counts['title'] == 0 and counts['abstract'] == 0:
        continue
    journal_scatter_data.append({
        'journal': journal,
        'title_count': counts['title'],
        'abstract_count': counts['abstract'],
        'both_count': counts['both'],
        'total_count': counts['total'],
        'title_only': max(counts['title'] - counts['both'], 0),
        'abstract_only': max(counts['abstract'] - counts['both'], 0)
    })

journal_scatter_data.sort(key=lambda item: item['total_count'], reverse=True)

year_scatter_data = []
for year, counts in sorted(year_term_totals.items()):
    if counts['title'] == 0 and counts['abstract'] == 0:
        continue
    year_scatter_data.append({
        'year': year,
        'title_count': counts['title'],
        'abstract_count': counts['abstract'],
        'both_count': counts['both'],
        'total_count': counts['total'],
        'title_only': max(counts['title'] - counts['both'], 0),
        'abstract_only': max(counts['abstract'] - counts['both'], 0)
    })

print("Generating HTML...")

# Generate HTML
html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Word Frequency Analysis Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ color: #333; margin-bottom: 10px; font-size: 2em; }}
        .last-updated {{ color: #999; font-size: 0.9em; margin-bottom: 30px; }}
        h2 {{ color: #555; margin: 30px 0 15px; font-size: 1.5em; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .card h3 {{ color: #666; font-size: 0.9em; text-transform: uppercase; margin-bottom: 10px; }}
        .card .value {{ font-size: 2.5em; font-weight: bold; color: #2196F3; }}
        .card .subvalue {{ font-size: 0.9em; color: #999; margin-top: 5px; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #eee; }}
        th {{ background: #f8f8f8; font-weight: 600; color: #666; text-transform: uppercase; font-size: 0.85em; }}
        tr:last-child td {{ border-bottom: none; }}
        .chart-container {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 30px; }}
        .chart-container h3 {{ margin-bottom: 15px; font-size: 1.1em; color: #555; }}
        .chart-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .chart-grid .chart-container {{ margin-bottom: 0; }}
        canvas {{ max-height: 400px; }}
        .progress-bar {{
            width: 100%;
            height: 8px;
            background: #eee;
            border-radius: 4px;
            overflow: hidden;
            margin-top: 10px;
        }}
        .progress-bar-fill {{
            height: 100%;
            background: #2196F3;
            transition: width 0.3s ease;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Word Frequency Analysis Dashboard</h1>
        <div class="last-updated">Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | <a href="journals.html" style="color: #2196F3;">View All Genetics Journals</a> | <a href="tokens.html" style="color: #2196F3;">Token Usage</a> | <a href="diagnostics.html" style="color: #2196F3;">Batch Diagnostics</a></div>

        <h2>Progress Overview</h2>
        <div class="grid">
            <div class="card">
                <h3>Total Articles</h3>
                <div class="value">{progress_data['total_articles']:,}</div>
                <div class="subvalue">{progress_data['processed_articles']:,} processed</div>
                <div class="progress-bar">
                    <div class="progress-bar-fill" style="width: {progress_data['processing_percentage']:.1f}%"></div>
                </div>
            </div>
            <div class="card">
                <h3>Processing Rate</h3>
                <div class="value">{progress_data['processing_percentage']:.1f}%</div>
                <div class="subvalue">{progress_data['unprocessed_articles']:,} remaining</div>
            </div>
            <div class="card">
                <h3>Est. Completion</h3>
                <div class="value">{"N/A" if completion_date is None else completion_date.strftime("%b %d, %Y")}</div>
                <div class="subvalue">{"No data yet" if completion_date is None else completion_date.strftime("%H:%M UTC")}</div>
            </div>
        </div>

        <h2>Token Usage</h2>
        <div class="grid">
            <div class="card">
                <h3>All Time</h3>
                <div class="value">{token_data['all_time']['total_tokens']:,}</div>
                <div class="subvalue">tokens</div>
            </div>
            <div class="card">
                <h3>Last 24 Hours</h3>
                <div class="value">{token_data['last_24h']['total_tokens']:,}</div>
                <div class="subvalue">tokens</div>
            </div>
            <div class="card">
                <h3>Batch Waiting (24h)</h3>
                <div class="value">{waiting_hours:.1f}h</div>
                <div class="subvalue">{batch_utilization:.1f}% active processing</div>
            </div>
        </div>

        <h2>Articles by Journal</h2>
        <table>
            <thead>
                <tr>
                    <th>Journal</th>
                    <th>Total</th>
                    <th>Processed</th>
                    <th>Progress</th>
                </tr>
            </thead>
            <tbody>
"""

for j in journal_stats:
    pct = (j['processed'] / j['total'] * 100) if j['total'] > 0 else 0
    html_content += f"""
                <tr>
                    <td>{j['journal']}</td>
                    <td>{j['total']:,}</td>
                    <td>{j['processed']:,}</td>
                    <td>{pct:.1f}%</td>
                </tr>
"""

runtime_seconds = time.time() - start_time

html_content += f"""
            </tbody>
        </table>

        <h2>References by Year</h2>
        <div class="chart-container">
            <canvas id="yearChart"></canvas>
        </div>

        <h2>Terminology Usage Proportions</h2>
        <div class="chart-container">
            <canvas id="termOverlayChart"></canvas>
        </div>

        <div class="chart-grid">
            <div class="chart-container">
                <h3>Caucasian Terminology</h3>
                <canvas id="caucasianChart" height="280"></canvas>
            </div>
            <div class="chart-container">
                <h3>White Terminology</h3>
                <canvas id="whiteChart" height="280"></canvas>
            </div>
            <div class="chart-container">
                <h3>European Terminology</h3>
                <canvas id="europeanChart" height="280"></canvas>
            </div>
            <div class="chart-container">
                <h3>Other Terminology</h3>
                <canvas id="otherChart" height="280"></canvas>
            </div>
        </div>

        <h2>Articles Mentioning Any Terminology</h2>
        <div class="chart-container">
            <canvas id="anyProportionChart"></canvas>
        </div>

        <h2>Title vs Abstract Mentions by Journal</h2>
        <div class="chart-container">
            <canvas id="journalScatterChart"></canvas>
        </div>

        <h2>Title vs Abstract Mentions by Year</h2>
        <div class="chart-container">
            <canvas id="yearScatterChart"></canvas>
        </div>

        <h2>System Information</h2>
        <div class="card">
            <h3>Dashboard Generation Time</h3>
            <div class="value">{runtime_seconds:.2f}s</div>
            <div class="subvalue">Last run: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
        </div>
        <br>

        <h2>References by Journal and Year</h2>
        <div class="chart-container">
            <canvas id="journalYearChart"></canvas>
        </div>
    </div>

    <script>
        const termYearData = """ + json.dumps(term_year_data, default=json_default) + """;
        const journalScatterData = """ + json.dumps(journal_scatter_data, default=json_default) + """;
        const yearScatterData = """ + json.dumps(year_scatter_data, default=json_default) + """;
        const byYearData = """ + json.dumps(by_year, default=json_default) + """;
        const byJournalYearData = """ + json.dumps(by_journal_year_final, default=json_default) + """;

        // Year chart
        const yearCtx = document.getElementById('yearChart').getContext('2d');
        const yearLabels = byYearData.map(d => d.year);
        const referenceCounts = byYearData.map(d => d.count || 0);
        const totalCounts = byYearData.map(d => d.total_articles || 0);
        const maxReference = referenceCounts.length ? Math.max(...referenceCounts) : 0;
        const maxTotal = totalCounts.length ? Math.max(...totalCounts) : 0;

        new Chart(yearCtx, {
            type: 'line',
            data: {
                labels: yearLabels,
                datasets: [
                    {
                        label: 'Articles with Race References',
                        data: referenceCounts,
                        borderColor: '#2196F3',
                        backgroundColor: 'rgba(33, 150, 243, 0.1)',
                        tension: 0.1,
                        fill: true,
                        yAxisID: 'yReferences'
                    },
                    {
                        label: 'Total Processed Articles',
                        data: totalCounts,
                        borderColor: '#4CAF50',
                        backgroundColor: 'rgba(76, 175, 80, 0.15)',
                        tension: 0.1,
                        fill: true,
                        yAxisID: 'yTotal'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: true }
                },
                scales: {
                    yReferences: {
                        type: 'linear',
                        position: 'left',
                        beginAtZero: true,
                        title: { display: true, text: 'Articles with race references' },
                        suggestedMax: maxReference > 0 ? Math.ceil(maxReference * 1.1) : 10
                    },
                    yTotal: {
                        type: 'linear',
                        position: 'right',
                        beginAtZero: true,
                        title: { display: true, text: 'Total processed articles' },
                        grid: { drawOnChartArea: false },
                        suggestedMax: maxTotal > 0 ? Math.ceil(maxTotal * 1.1) : 10
                    },
                    x: { title: { display: true, text: 'Year' } }
                }
            }
        });

        // Terminology overlay chart
        const overlayCtx = document.getElementById('termOverlayChart').getContext('2d');
        const overlayConfigs = [
            { key: 'caucasian_pct', label: 'Caucasian', color: '#F44336', background: 'rgba(244, 67, 54, 0.15)' },
            { key: 'white_pct', label: 'White', color: '#FF9800', background: 'rgba(255, 152, 0, 0.15)' },
            { key: 'european_pct', label: 'European', color: '#2196F3', background: 'rgba(33, 150, 243, 0.15)' },
            { key: 'other_pct', label: 'Other', color: '#9C27B0', background: 'rgba(156, 39, 176, 0.15)' }
        ];
        const overlayDatasets = overlayConfigs.map(cfg => ({
            label: cfg.label,
            data: termYearData.map(d => typeof d[cfg.key] === 'number' ? d[cfg.key] : 0),
            borderColor: cfg.color,
            backgroundColor: cfg.background,
            tension: 0.1,
            fill: true,
            spanGaps: true
        }));
        const overlayValues = [];
        overlayDatasets.forEach(dataset => {
            dataset.data.forEach(value => overlayValues.push(value));
        });
        const overlayMax = overlayValues.length ? Math.max(...overlayValues) : 0;
        const overlaySuggestedMax = overlayMax > 0 ? Math.min(100, Math.ceil(overlayMax / 5) * 5 + 5) : 10;
        new Chart(overlayCtx, {
            type: 'line',
            data: {
                labels: termYearData.map(d => d.year),
                datasets: overlayDatasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: true, position: 'bottom' },
                    tooltip: {
                        callbacks: {
                            label: context => context.dataset.label + ': ' + context.parsed.y.toFixed(2) + '%'
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        suggestedMax: overlaySuggestedMax,
                        ticks: { callback: value => value + '%' },
                        title: { display: true, text: 'Percent of processed articles' }
                    },
                    x: { title: { display: true, text: 'Year' } }
                }
            }
        });

        function renderPercentLineChart(canvasId, label, dataKey, color, background, options = {}) {
            const canvas = document.getElementById(canvasId);
            if (!canvas) return;
            const ctx = canvas.getContext('2d');
            const values = termYearData.map(d => typeof d[dataKey] === 'number' ? d[dataKey] : 0);
            const maxValue = values.length ? Math.max(...values) : 0;
            const paddedMax = maxValue > 0 ? Math.min(100, Math.ceil(maxValue / 5) * 5 + 5) : 10;
            new Chart(ctx, {
                type: 'line',
                data: {
                    labels: termYearData.map(d => d.year),
                    datasets: [{
                        label,
                        data: values,
                        borderColor: color,
                        backgroundColor: background,
                        tension: 0.1,
                        fill: true,
                        spanGaps: true
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {
                        legend: { display: Object.prototype.hasOwnProperty.call(options, 'showLegend') ? options.showLegend : false },
                        tooltip: {
                            callbacks: {
                                label: context => label + ': ' + context.parsed.y.toFixed(2) + '%'
                            }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            suggestedMax: paddedMax,
                            ticks: { callback: value => value + '%' },
                            title: {
                                display: Boolean(options.yTitle),
                                text: options.yTitle || ''
                            }
                        },
                        x: { title: { display: true, text: 'Year' } }
                    }
                }
            });
        }

        renderPercentLineChart('caucasianChart', 'Caucasian Terminology', 'caucasian_pct', '#F44336', 'rgba(244, 67, 54, 0.18)');
        renderPercentLineChart('whiteChart', 'White Terminology', 'white_pct', '#FF9800', 'rgba(255, 152, 0, 0.2)');
        renderPercentLineChart('europeanChart', 'European Terminology', 'european_pct', '#2196F3', 'rgba(33, 150, 243, 0.18)');
        renderPercentLineChart('otherChart', 'Other Terminology', 'other_pct', '#9C27B0', 'rgba(156, 39, 176, 0.18)');
        renderPercentLineChart('anyProportionChart', 'Any Terminology', 'any_pct', '#4CAF50', 'rgba(76, 175, 80, 0.18)', { yTitle: 'Percent of processed articles' });

        const journalScatterCtx = document.getElementById('journalScatterChart').getContext('2d');
        new Chart(journalScatterCtx, {
            type: 'scatter',
            data: {
                datasets: [{
                    label: 'Journals',
                    data: journalScatterData.map(item => ({
                        x: item.title_count,
                        y: item.abstract_count,
                        label: item.journal,
                        total: item.total_count,
                        both: item.both_count,
                        titleOnly: item.title_only,
                        abstractOnly: item.abstract_only
                    })),
                    backgroundColor: 'rgba(33, 150, 243, 0.45)',
                    borderColor: '#2196F3',
                    pointRadius: context => Math.min(12, 4 + Math.sqrt(context.raw.total || 0)),
                    pointHoverRadius: context => Math.min(14, 5 + Math.sqrt(context.raw.total || 0))
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: context => {
                                const point = context.raw;
                                return [
                                    point.label,
                                    'Title mentions: ' + point.x,
                                    'Abstract mentions: ' + point.y,
                                    'Both title & abstract: ' + point.both,
                                    'Title only: ' + point.titleOnly,
                                    'Abstract only: ' + point.abstractOnly,
                                    'Total terminology articles: ' + point.total
                                ];
                            }
                        }
                    }
                },
                scales: {
                    x: { beginAtZero: true, title: { display: true, text: 'Articles with race terms in titles' } },
                    y: { beginAtZero: true, title: { display: true, text: 'Articles with race terms in abstracts' } }
                }
            }
        });

        const yearScatterCtx = document.getElementById('yearScatterChart').getContext('2d');
        new Chart(yearScatterCtx, {
            type: 'scatter',
            data: {
                datasets: [{
                    label: 'Years',
                    data: yearScatterData.map(item => ({
                        x: item.title_count,
                        y: item.abstract_count,
                        label: item.year,
                        total: item.total_count,
                        both: item.both_count,
                        titleOnly: item.title_only,
                        abstractOnly: item.abstract_only
                    })),
                    backgroundColor: 'rgba(76, 175, 80, 0.5)',
                    borderColor: '#4CAF50',
                    pointRadius: context => Math.min(12, 4 + Math.sqrt(context.raw.total || 0)),
                    pointHoverRadius: context => Math.min(14, 5 + Math.sqrt(context.raw.total || 0))
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: context => {
                                const point = context.raw;
                                return [
                                    'Year ' + point.label,
                                    'Title mentions: ' + point.x,
                                    'Abstract mentions: ' + point.y,
                                    'Both title & abstract: ' + point.both,
                                    'Title only: ' + point.titleOnly,
                                    'Abstract only: ' + point.abstractOnly,
                                    'Total terminology articles: ' + point.total
                                ];
                            }
                        }
                    }
                },
                scales: {
                    x: { beginAtZero: true, title: { display: true, text: 'Articles with race terms in titles' } },
                    y: { beginAtZero: true, title: { display: true, text: 'Articles with race terms in abstracts' } }
                }
            }
        });

        // Journal-year chart
        const journalYearCtx = document.getElementById('journalYearChart').getContext('2d');
        const journalNames = [...new Set(byJournalYearData.map(d => d.journal))].sort();
        const colors = [
            '#2196F3', '#4CAF50', '#FF9800', '#F44336', '#9C27B0',
            '#00BCD4', '#FFEB3B', '#795548', '#607D8B', '#E91E63'
        ];

        const datasets = journalNames.map((journal, i) => {
            const journalData = byJournalYearData.filter(d => d.journal === journal);
            return {
                label: journal,
                data: journalData.map(d => ({ x: d.year, y: d.count })),
                borderColor: colors[i % colors.length],
                backgroundColor: colors[i % colors.length] + '20',
                tension: 0.1
            };
        });

        new Chart(journalYearCtx, {
            type: 'line',
            data: { datasets },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: true, position: 'bottom' }
                },
                scales: {
                    x: { type: 'linear', title: { display: true, text: 'Year' } },
                    y: { beginAtZero: true, title: { display: true, text: 'Count' } }
                }
            }
        });
    </script>
</body>
</html>
"""

# Write HTML file
output_path = os.path.join(args.output_dir, 'index.html')
with open(output_path, 'w') as f:
    f.write(html_content)

print("Generating journals page...")

# Get all journals from the journals table
execute_query("SELECT name, enabled FROM languageingenetics.journals ORDER BY name")
tracked_journals = {row['name']: row['enabled'] for row in cursor.fetchall()}

# Extend journals_mv_data with additional fields if we need them for the journals page
# (we already queried journals_mv at the beginning of the script)
if using_mv:
    # Re-query with additional fields for the journals page
    try:
        execute_query("""
            SELECT
                journal_name,
                article_count,
                earliest_year,
                latest_year,
                articles_with_abstract,
                abstract_percentage,
                total_citations,
                avg_citations_per_article,
                total_references,
                publication_types,
                sample_issn
            FROM public.journals_mv
            WHERE journal_name ILIKE '%genetic%'
            ORDER BY article_count DESC, journal_name
        """)
        journals_mv_data = {row['journal_name']: row for row in cursor.fetchall()}
    except psycopg2.Error:
        conn.rollback()
        # If it fails now, just continue with what we have
        pass

# Get race terminology breakdown per journal from processed files
execute_query("""
    SELECT
        (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title'->>0) as journal,
        COUNT(*) as processed_count,
        COUNT(*) FILTER (WHERE f.caucasian = true) as caucasian_count,
        COUNT(*) FILTER (WHERE f.white = true) as white_count,
        COUNT(*) FILTER (WHERE f.european = true) as european_count,
        COUNT(*) FILTER (WHERE f.other = true) as other_count,
        COUNT(*) FILTER (WHERE f.caucasian = true OR f.white = true OR f.european = true OR f.other = true) as any_terminology_count,
        MIN(f.pub_year) as earliest_processed_year,
        MAX(f.pub_year) as latest_processed_year,
        AVG(f.prompt_tokens + f.completion_tokens) as avg_tokens,
        COUNT(*) FILTER (WHERE f.has_abstract = true) as processed_with_abstract
    FROM languageingenetics.files f
    JOIN public.raw_text_data r ON f.article_id = r.id
    WHERE f.processed = true
    GROUP BY journal
""")
processed_stats = {row['journal']: row for row in cursor.fetchall() if row['journal']}

# Get processing counts for tracked journals
execute_query("""
    SELECT
        (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title'->>0) as journal,
        COUNT(*) as processed_count
    FROM languageingenetics.files f
    JOIN public.raw_text_data r ON f.article_id = r.id
    WHERE f.processed = true
    GROUP BY journal
""")
processed_by_journal = {row['journal']: row['processed_count'] for row in cursor.fetchall() if row['journal']}

# Build the comprehensive data structure for the journals page
journals_data = []
for journal_name, mv_row in journals_mv_data.items():
    journal_name = journal_name.strip() if journal_name else journal_name

    # Check if this journal is tracked
    if journal_name in tracked_journals:
        status = "Active" if tracked_journals[journal_name] else "Inactive"
        tracked = True
    else:
        status = "Not Tracked"
        tracked = False

    # Get processed statistics
    proc_stats = processed_stats.get(journal_name, {})
    processed_count = processed_by_journal.get(journal_name, 0)

    journals_data.append({
        'name': journal_name,
        'tracked': tracked,
        'status': status,
        'article_count': mv_row.get('article_count', 0),
        'earliest_year': mv_row.get('earliest_year'),
        'latest_year': mv_row.get('latest_year'),
        'abstract_percentage': mv_row.get('abstract_percentage'),
        'avg_citations': mv_row.get('avg_citations_per_article'),
        'processed_count': processed_count,
        'caucasian_count': proc_stats.get('caucasian_count', 0),
        'white_count': proc_stats.get('white_count', 0),
        'european_count': proc_stats.get('european_count', 0),
        'other_count': proc_stats.get('other_count', 0),
        'any_terminology_count': proc_stats.get('any_terminology_count', 0),
        'hit_rate': (proc_stats.get('any_terminology_count', 0) / processed_count * 100) if processed_count > 0 else 0,
        'avg_tokens': round(proc_stats.get('avg_tokens', 0)) if proc_stats.get('avg_tokens') else 0
    })

# Generate journals HTML page with comprehensive statistics
journals_html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Genetics Journals - Word Frequency Analysis</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }}
        .container {{ max-width: 1800px; margin: 0 auto; }}
        h1 {{ color: #333; margin-bottom: 10px; font-size: 2em; }}
        h2 {{ color: #555; margin: 30px 0 15px; font-size: 1.5em; }}
        .last-updated {{ color: #999; font-size: 0.9em; margin-bottom: 30px; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow-x: auto;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            font-size: 0.9em;
        }}
        th, td {{ padding: 10px 8px; text-align: left; border-bottom: 1px solid #eee; white-space: nowrap; }}
        th {{
            background: #f8f8f8;
            font-weight: 600;
            color: #666;
            text-transform: uppercase;
            font-size: 0.8em;
            cursor: pointer;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        th:hover {{ background: #e8e8e8; }}
        th.sortable::after {{ content: ' ⇅'; opacity: 0.3; }}
        th.sorted-asc::after {{ content: ' ↑'; opacity: 1; }}
        th.sorted-desc::after {{ content: ' ↓'; opacity: 1; }}
        tr:last-child td {{ border-bottom: none; }}
        tr:hover {{ background: #fafafa; }}
        .status-active {{ color: #4CAF50; font-weight: 600; }}
        .status-inactive {{ color: #FF9800; font-weight: 600; }}
        .status-not-tracked {{ color: #999; }}
        a {{ color: #2196F3; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .summary {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 20px; margin-top: 15px; }}
        .summary-item {{ text-align: center; }}
        .summary-value {{ font-size: 2em; font-weight: bold; color: #2196F3; }}
        .summary-label {{ color: #666; font-size: 0.85em; margin-top: 5px; }}
        .numeric {{ text-align: right; font-variant-numeric: tabular-nums; color: #333; }}
        .terminology-bar {{
            display: inline-flex;
            height: 20px;
            width: 100px;
            background: #eee;
            border-radius: 3px;
            overflow: hidden;
        }}
        .term-caucasian {{ background: #F44336; }}
        .term-white {{ background: #FF9800; }}
        .term-european {{ background: #2196F3; }}
        .term-other {{ background: #9C27B0; }}
        .chart-container {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 30px; }}
        canvas {{ max-height: 500px; }}
        .filters {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            align-items: center;
        }}
        .filter-group {{ display: flex; align-items: center; gap: 8px; }}
        .filter-group label {{ color: #666; font-size: 0.9em; font-weight: 600; }}
        .filter-group select, .filter-group input {{
            padding: 6px 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 0.9em;
        }}
        .table-wrapper {{ overflow-x: auto; }}
        .hit-rate-high {{ color: #4CAF50; font-weight: 600; }}
        .hit-rate-medium {{ color: #FF9800; font-weight: 600; }}
        .hit-rate-low {{ color: #999; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Genetics Journals</h1>
        <div class="last-updated">Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | <a href="index.html">Back to Dashboard</a></div>

        <div class="summary">
            <h3>Summary</h3>
            <div class="summary-grid">
                <div class="summary-item">
                    <div class="summary-value">{len(journals_data)}</div>
                    <div class="summary-label">Total Journals</div>
                </div>
                <div class="summary-item">
                    <div class="summary-value">{len([j for j in journals_data if j['tracked']])}</div>
                    <div class="summary-label">Tracked</div>
                </div>
                <div class="summary-item">
                    <div class="summary-value">{len([j for j in journals_data if j['status'] == 'Active'])}</div>
                    <div class="summary-label">Active</div>
                </div>
                <div class="summary-item">
                    <div class="summary-value">{sum(j['article_count'] for j in journals_data):,}</div>
                    <div class="summary-label">Total Articles</div>
                </div>
                <div class="summary-item">
                    <div class="summary-value">{sum(j['processed_count'] for j in journals_data):,}</div>
                    <div class="summary-label">Processed</div>
                </div>
                <div class="summary-item">
                    <div class="summary-value">{sum(j['any_terminology_count'] for j in journals_data):,}</div>
                    <div class="summary-label">With Terminology</div>
                </div>
            </div>
        </div>

        <div class="filters">
            <div class="filter-group">
                <label>Status:</label>
                <select id="statusFilter">
                    <option value="all">All</option>
                    <option value="Active">Active</option>
                    <option value="Inactive">Inactive</option>
                    <option value="Not Tracked">Not Tracked</option>
                </select>
            </div>
            <div class="filter-group">
                <label>Min Abstract %:</label>
                <input type="number" id="abstractFilter" min="0" max="100" value="0" style="width: 80px;">
            </div>
            <div class="filter-group">
                <label>Search:</label>
                <input type="text" id="searchFilter" placeholder="Journal name..." style="width: 200px;">
            </div>
            <div class="filter-group">
                <button onclick="resetFilters()" style="padding: 6px 15px; background: #2196F3; color: white; border: none; border-radius: 4px; cursor: pointer;">Reset</button>
            </div>
        </div>

        <h2>Terminology Analysis by Journal</h2>
        <div class="chart-container">
            <canvas id="terminologyChart"></canvas>
        </div>

        <h2>Journal Details</h2>
        <div class="table-wrapper">
            <table id="journalsTable">
                <thead>
                    <tr>
                        <th class="sortable" data-column="name">Journal Name</th>
                        <th class="sortable" data-column="status">Status</th>
                        <th class="sortable numeric" data-column="article_count">Total Articles</th>
                        <th class="sortable numeric" data-column="processed_count">Processed</th>
                        <th class="sortable numeric" data-column="earliest_year">Year Range</th>
                        <th class="sortable numeric" data-column="abstract_percentage">Abstract %</th>
                        <th class="sortable numeric" data-column="avg_citations">Avg Citations</th>
                        <th class="sortable numeric" data-column="hit_rate">Hit Rate</th>
                        <th>Terminology</th>
                        <th class="sortable numeric" data-column="caucasian_count">Caucasian</th>
                        <th class="sortable numeric" data-column="white_count">White</th>
                        <th class="sortable numeric" data-column="european_count">European</th>
                        <th class="sortable numeric" data-column="other_count">Other</th>
                    </tr>
                </thead>
                <tbody id="journalsTableBody">
"""

for journal in journals_data:
    status_class = f"status-{journal['status'].lower().replace(' ', '-')}"
    year_range = f"{journal.get('earliest_year') or '?'}–{journal.get('latest_year') or '?'}"
    abstract_pct = f"{journal.get('abstract_percentage', 0):.1f}%" if journal.get('abstract_percentage') is not None else "N/A"
    avg_cit = f"{journal.get('avg_citations', 0):.1f}" if journal.get('avg_citations') is not None else "N/A"

    hit_rate_class = "hit-rate-high" if journal['hit_rate'] > 10 else ("hit-rate-medium" if journal['hit_rate'] > 5 else "hit-rate-low")

    # Build terminology bar
    total_terms = journal['caucasian_count'] + journal['white_count'] + journal['european_count'] + journal['other_count']
    term_bar = ""
    if total_terms > 0:
        cauc_pct = journal['caucasian_count'] / total_terms * 100
        white_pct = journal['white_count'] / total_terms * 100
        euro_pct = journal['european_count'] / total_terms * 100
        other_pct = journal['other_count'] / total_terms * 100
        term_bar = '<div class="terminology-bar">'
        if cauc_pct > 0:
            term_bar += f'<div class="term-caucasian" style="width: {cauc_pct}%" title="Caucasian: {journal["caucasian_count"]}"></div>'
        if white_pct > 0:
            term_bar += f'<div class="term-white" style="width: {white_pct}%" title="White: {journal["white_count"]}"></div>'
        if euro_pct > 0:
            term_bar += f'<div class="term-european" style="width: {euro_pct}%" title="European: {journal["european_count"]}"></div>'
        if other_pct > 0:
            term_bar += f'<div class="term-other" style="width: {other_pct}%" title="Other: {journal["other_count"]}"></div>'
        term_bar += '</div>'

    journals_html += f"""
                <tr data-status="{journal['status']}" data-abstract="{journal.get('abstract_percentage', 0) or 0}" data-name="{journal['name'].lower()}">
                    <td>{journal['name']}</td>
                    <td class="{status_class}">{journal['status']}</td>
                    <td class="numeric">{journal['article_count']:,}</td>
                    <td class="numeric">{journal['processed_count']:,}</td>
                    <td class="numeric">{year_range}</td>
                    <td class="numeric">{abstract_pct}</td>
                    <td class="numeric">{avg_cit}</td>
                    <td class="numeric {hit_rate_class}">{journal['hit_rate']:.1f}%</td>
                    <td>{term_bar}</td>
                    <td class="numeric">{journal['caucasian_count']}</td>
                    <td class="numeric">{journal['white_count']}</td>
                    <td class="numeric">{journal['european_count']}</td>
                    <td class="numeric">{journal['other_count']}</td>
                </tr>
"""

journals_html += """
                </tbody>
            </table>
        </div>
    </div>

    <script>
        const journalsData = """ + json.dumps(journals_data, default=json_default) + """;

        // Terminology breakdown chart - top 15 journals by processed count
        const topJournals = journalsData
            .filter(j => j.processed_count > 0)
            .sort((a, b) => b.processed_count - a.processed_count)
            .slice(0, 15);

        const termCtx = document.getElementById('terminologyChart').getContext('2d');
        new Chart(termCtx, {
            type: 'bar',
            data: {
                labels: topJournals.map(j => j.name.length > 40 ? j.name.substring(0, 37) + '...' : j.name),
                datasets: [
                    {
                        label: 'Caucasian',
                        data: topJournals.map(j => j.caucasian_count),
                        backgroundColor: '#F44336'
                    },
                    {
                        label: 'White',
                        data: topJournals.map(j => j.white_count),
                        backgroundColor: '#FF9800'
                    },
                    {
                        label: 'European',
                        data: topJournals.map(j => j.european_count),
                        backgroundColor: '#2196F3'
                    },
                    {
                        label: 'Other',
                        data: topJournals.map(j => j.other_count),
                        backgroundColor: '#9C27B0'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                indexAxis: 'y',
                scales: {
                    x: { stacked: true, title: { display: true, text: 'Articles with Terminology' } },
                    y: { stacked: true }
                },
                plugins: {
                    legend: { display: true, position: 'top' },
                    title: { display: true, text: 'Race Terminology Usage by Journal (Top 15)' }
                }
            }
        });

        // Table sorting
        let currentSort = { column: 'article_count', ascending: false };

        document.querySelectorAll('th.sortable').forEach(th => {
            th.addEventListener('click', () => {
                const column = th.dataset.column;
                const ascending = currentSort.column === column ? !currentSort.ascending : false;
                currentSort = { column, ascending };

                // Update header classes
                document.querySelectorAll('th.sortable').forEach(h => {
                    h.classList.remove('sorted-asc', 'sorted-desc');
                });
                th.classList.add(ascending ? 'sorted-asc' : 'sorted-desc');

                // Sort and re-render
                sortTable(column, ascending);
            });
        });

        function sortTable(column, ascending) {
            const tbody = document.getElementById('journalsTableBody');
            const rows = Array.from(tbody.querySelectorAll('tr'));

            rows.sort((a, b) => {
                const aData = journalsData.find(j => j.name === a.cells[0].textContent);
                const bData = journalsData.find(j => j.name === b.cells[0].textContent);

                let aVal = aData[column];
                let bVal = bData[column];

                // Handle nulls
                if (aVal === null || aVal === undefined) aVal = ascending ? Infinity : -Infinity;
                if (bVal === null || bVal === undefined) bVal = ascending ? Infinity : -Infinity;

                if (typeof aVal === 'string') {
                    return ascending ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                }
                return ascending ? aVal - bVal : bVal - aVal;
            });

            tbody.innerHTML = '';
            rows.forEach(row => tbody.appendChild(row));
        }

        // Filtering
        function applyFilters() {
            const statusFilter = document.getElementById('statusFilter').value;
            const abstractFilter = parseFloat(document.getElementById('abstractFilter').value);
            const searchFilter = document.getElementById('searchFilter').value.toLowerCase();

            document.querySelectorAll('#journalsTableBody tr').forEach(row => {
                const status = row.dataset.status;
                const abstract = parseFloat(row.dataset.abstract);
                const name = row.dataset.name;

                const statusMatch = statusFilter === 'all' || status === statusFilter;
                const abstractMatch = abstract >= abstractFilter;
                const searchMatch = searchFilter === '' || name.includes(searchFilter);

                row.style.display = (statusMatch && abstractMatch && searchMatch) ? '' : 'none';
            });
        }

        document.getElementById('statusFilter').addEventListener('change', applyFilters);
        document.getElementById('abstractFilter').addEventListener('input', applyFilters);
        document.getElementById('searchFilter').addEventListener('input', applyFilters);

        function resetFilters() {
            document.getElementById('statusFilter').value = 'all';
            document.getElementById('abstractFilter').value = '0';
            document.getElementById('searchFilter').value = '';
            applyFilters();
        }
    </script>
</body>
</html>
"""

# Write journals HTML file
journals_output_path = os.path.join(args.output_dir, 'journals.html')
with open(journals_output_path, 'w') as f:
    f.write(journals_html)

print("Generating token usage page...")

# Get token usage data over time (daily aggregation)
execute_query("""
    SELECT
        DATE(when_processed) as date,
        COUNT(*) as articles_processed,
        SUM(prompt_tokens) as prompt_tokens,
        SUM(completion_tokens) as completion_tokens,
        SUM(prompt_tokens + completion_tokens) as total_tokens
    FROM languageingenetics.files
    WHERE processed = true AND when_processed IS NOT NULL
    GROUP BY DATE(when_processed)
    ORDER BY date
""")
daily_token_data = [dict(row) for row in cursor.fetchall()]

# Convert dates to strings for JSON serialization
for row in daily_token_data:
    row['date'] = row['date'].isoformat()

# Get cumulative token usage
cumulative_tokens = []
running_total = 0
for row in daily_token_data:
    running_total += row['total_tokens']
    cumulative_tokens.append({
        'date': row['date'],
        'cumulative_tokens': running_total
    })

# Get token usage by batch
execute_query("""
    SELECT
        b.id as batch_id,
        b.when_sent,
        b.when_retrieved,
        COUNT(*) as articles,
        SUM(f.prompt_tokens) as prompt_tokens,
        SUM(f.completion_tokens) as completion_tokens,
        SUM(f.prompt_tokens + f.completion_tokens) as total_tokens
    FROM languageingenetics.batches b
    JOIN languageingenetics.files f ON f.batch_id = b.id
    WHERE f.processed = true
    GROUP BY b.id, b.when_sent, b.when_retrieved
    ORDER BY b.when_sent
""")
batch_token_data = []
for row in cursor.fetchall():
    batch_token_data.append({
        'batch_id': row['batch_id'],
        'when_sent': row['when_sent'].isoformat() if row['when_sent'] else None,
        'when_retrieved': row['when_retrieved'].isoformat() if row['when_retrieved'] else None,
        'articles': row['articles'],
        'prompt_tokens': row['prompt_tokens'],
        'completion_tokens': row['completion_tokens'],
        'total_tokens': row['total_tokens']
    })

# Calculate cost estimates (GPT-4 pricing as example)
# Adjust these rates based on actual OpenAI pricing
PROMPT_COST_PER_1M = 5.00  # $5 per 1M prompt tokens
COMPLETION_COST_PER_1M = 15.00  # $15 per 1M completion tokens

total_prompt_cost = (all_time_prompt / 1_000_000) * PROMPT_COST_PER_1M
total_completion_cost = (all_time_completion / 1_000_000) * COMPLETION_COST_PER_1M
total_cost = total_prompt_cost + total_completion_cost

# Generate token usage HTML
tokens_html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Token Usage - Word Frequency Analysis</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ color: #333; margin-bottom: 10px; font-size: 2em; }}
        h2 {{ color: #555; margin: 30px 0 15px; font-size: 1.5em; }}
        .last-updated {{ color: #999; font-size: 0.9em; margin-bottom: 30px; }}
        a {{ color: #2196F3; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .card h3 {{ color: #666; font-size: 0.9em; text-transform: uppercase; margin-bottom: 10px; }}
        .card .value {{ font-size: 2.5em; font-weight: bold; color: #2196F3; }}
        .card .subvalue {{ font-size: 0.9em; color: #999; margin-top: 5px; }}
        .chart-container {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 30px; }}
        canvas {{ max-height: 400px; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #eee; }}
        th {{ background: #f8f8f8; font-weight: 600; color: #666; text-transform: uppercase; font-size: 0.85em; }}
        tr:last-child td {{ border-bottom: none; }}
        .numeric {{ text-align: right; font-variant-numeric: tabular-nums; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Token Usage Analysis</h1>
        <div class="last-updated">Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | <a href="index.html">Back to Dashboard</a></div>

        <h2>Overall Statistics</h2>
        <div class="grid">
            <div class="card">
                <h3>Total Tokens</h3>
                <div class="value">{(all_time_prompt + all_time_completion):,}</div>
                <div class="subvalue">{processed_articles:,} articles processed</div>
            </div>
            <div class="card">
                <h3>Prompt Tokens</h3>
                <div class="value">{all_time_prompt:,}</div>
                <div class="subvalue">{(all_time_prompt / processed_articles):.0f} per article</div>
            </div>
            <div class="card">
                <h3>Completion Tokens</h3>
                <div class="value">{all_time_completion:,}</div>
                <div class="subvalue">{(all_time_completion / processed_articles):.0f} per article</div>
            </div>
            <div class="card">
                <h3>Estimated Cost</h3>
                <div class="value">${total_cost:,.2f}</div>
                <div class="subvalue">${(total_cost / processed_articles):.4f} per article</div>
            </div>
        </div>

        <h2>Daily Token Usage</h2>
        <div class="chart-container">
            <canvas id="dailyTokenChart"></canvas>
        </div>

        <h2>Cumulative Token Usage</h2>
        <div class="chart-container">
            <canvas id="cumulativeTokenChart"></canvas>
        </div>

        <h2>Token Usage by Batch</h2>
        <div class="chart-container">
            <canvas id="batchTokenChart"></canvas>
        </div>

        <h2>Recent Batches</h2>
        <table>
            <thead>
                <tr>
                    <th>Batch ID</th>
                    <th>Sent</th>
                    <th>Retrieved</th>
                    <th class="numeric">Articles</th>
                    <th class="numeric">Prompt Tokens</th>
                    <th class="numeric">Completion Tokens</th>
                    <th class="numeric">Total Tokens</th>
                </tr>
            </thead>
            <tbody>
"""

# Show last 20 batches
for batch in batch_token_data[-20:]:
    sent = batch['when_sent'][:10] if batch['when_sent'] else 'N/A'
    retrieved = batch['when_retrieved'][:10] if batch['when_retrieved'] else 'N/A'
    tokens_html += f"""
                <tr>
                    <td>{batch['batch_id']}</td>
                    <td>{sent}</td>
                    <td>{retrieved}</td>
                    <td class="numeric">{batch['articles']:,}</td>
                    <td class="numeric">{batch['prompt_tokens']:,}</td>
                    <td class="numeric">{batch['completion_tokens']:,}</td>
                    <td class="numeric">{batch['total_tokens']:,}</td>
                </tr>
"""

tokens_html += f"""
            </tbody>
        </table>
    </div>

    <script>
        const dailyData = """ + json.dumps(daily_token_data, default=json_default) + """;
        const cumulativeData = """ + json.dumps(cumulative_tokens, default=json_default) + """;
        const batchData = """ + json.dumps(batch_token_data, default=json_default) + """;

        // Daily token usage chart
        const dailyCtx = document.getElementById('dailyTokenChart').getContext('2d');
        new Chart(dailyCtx, {{
            type: 'bar',
            data: {{
                labels: dailyData.map(d => d.date),
                datasets: [
                    {{
                        label: 'Prompt Tokens',
                        data: dailyData.map(d => d.prompt_tokens),
                        backgroundColor: 'rgba(33, 150, 243, 0.7)',
                        stack: 'stack0'
                    }},
                    {{
                        label: 'Completion Tokens',
                        data: dailyData.map(d => d.completion_tokens),
                        backgroundColor: 'rgba(76, 175, 80, 0.7)',
                        stack: 'stack0'
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                plugins: {{
                    legend: {{ display: true, position: 'top' }},
                    title: {{ display: true, text: 'Daily Token Usage (Stacked)' }}
                }},
                scales: {{
                    x: {{
                        type: 'time',
                        time: {{ unit: 'day' }},
                        title: {{ display: true, text: 'Date' }}
                    }},
                    y: {{
                        beginAtZero: true,
                        title: {{ display: true, text: 'Tokens' }}
                    }}
                }}
            }}
        }});

        // Cumulative token usage chart
        const cumulativeCtx = document.getElementById('cumulativeTokenChart').getContext('2d');
        new Chart(cumulativeCtx, {{
            type: 'line',
            data: {{
                labels: cumulativeData.map(d => d.date),
                datasets: [{{
                    label: 'Cumulative Tokens',
                    data: cumulativeData.map(d => d.cumulative_tokens),
                    borderColor: '#2196F3',
                    backgroundColor: 'rgba(33, 150, 243, 0.1)',
                    tension: 0.1,
                    fill: true
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                plugins: {{
                    legend: {{ display: true, position: 'top' }},
                    title: {{ display: true, text: 'Cumulative Token Usage Over Time' }}
                }},
                scales: {{
                    x: {{
                        type: 'time',
                        time: {{ unit: 'day' }},
                        title: {{ display: true, text: 'Date' }}
                    }},
                    y: {{
                        beginAtZero: true,
                        title: {{ display: true, text: 'Total Tokens' }}
                    }}
                }}
            }}
        }});

        // Batch token usage chart
        const batchCtx = document.getElementById('batchTokenChart').getContext('2d');
        const last30Batches = batchData.slice(-30);
        new Chart(batchCtx, {{
            type: 'bar',
            data: {{
                labels: last30Batches.map(d => `Batch ${{d.batch_id}}`),
                datasets: [
                    {{
                        label: 'Prompt Tokens',
                        data: last30Batches.map(d => d.prompt_tokens),
                        backgroundColor: 'rgba(33, 150, 243, 0.7)',
                        stack: 'stack0'
                    }},
                    {{
                        label: 'Completion Tokens',
                        data: last30Batches.map(d => d.completion_tokens),
                        backgroundColor: 'rgba(76, 175, 80, 0.7)',
                        stack: 'stack0'
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                plugins: {{
                    legend: {{ display: true, position: 'top' }},
                    title: {{ display: true, text: 'Token Usage by Batch (Last 30)' }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        title: {{ display: true, text: 'Tokens' }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""

# Write token usage HTML file
tokens_output_path = os.path.join(args.output_dir, 'tokens.html')
with open(tokens_output_path, 'w') as f:
    f.write(tokens_html)

print("Generating batch diagnostics page...")

execute_query("""
    SELECT id, when_created, when_sent, when_retrieved
    FROM languageingenetics.batches
    ORDER BY id DESC
    LIMIT 15
""")
recent_batch_rows = [dict(row) for row in cursor.fetchall()]

diagnostics_by_batch = {
    row['id']: {
        'meta': row,
        'events': {},
        'summary': None
    }
    for row in recent_batch_rows
}

if recent_batch_rows:
    batch_ids = [row['id'] for row in recent_batch_rows]
    execute_query(
        """
        SELECT
            bd.batch_id,
            bd.article_id,
            bd.event_type,
            bd.details,
            bd.created_at
        FROM languageingenetics.batch_diagnostics bd
        WHERE bd.batch_id = ANY(%s)
        ORDER BY bd.batch_id DESC, bd.created_at, bd.id
        """,
        [batch_ids]
    )
    for row in cursor.fetchall():
        batch_data = diagnostics_by_batch.get(row['batch_id'])
        if not batch_data:
            continue
        details = dict(row['details']) if row['details'] else {}
        if row['event_type'] == 'summary':
            batch_data['summary'] = details
            continue
        reason = details.get('reason')
        if row['event_type'] == 'submitted':
            reason = 'with_abstract' if details.get('has_abstract') else 'without_abstract'
        key = (row['event_type'], reason)
        event_entry = batch_data['events'].setdefault(
            key,
            {
                'event_type': row['event_type'],
                'reason': reason,
                'count': 0,
                'sample_article_ids': []
            }
        )
        event_entry['count'] += 1
        if row['article_id'] is not None and len(event_entry['sample_article_ids']) < 5:
            event_entry['sample_article_ids'].append(row['article_id'])
else:
    diagnostics_by_batch = {}


def format_timestamp(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "—"


def humanize(text):
    if not text:
        return '—'
    return str(text).replace('_', ' ').title()


diagnostics_html = f'''<!DOCTYPE html>
<html>
<head>
    <title>Batch Diagnostics - Word Frequency Analysis</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ color: #333; margin-bottom: 10px; font-size: 2em; }}
        h2 {{ color: #555; margin: 30px 0 15px; font-size: 1.4em; }}
        .last-updated {{ color: #999; font-size: 0.9em; margin-bottom: 30px; }}
        .batch-card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .batch-meta {{ color: #666; font-size: 0.9em; margin-bottom: 15px; display: flex; flex-wrap: wrap; gap: 15px; }}
        .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 20px; }}
        .metric {{
            background: #f8f8f8;
            border-radius: 6px;
            padding: 12px;
        }}
        .metric-label {{ color: #666; font-size: 0.8em; text-transform: uppercase; letter-spacing: 0.03em; margin-bottom: 4px; }}
        .metric-value {{ font-size: 1.6em; font-weight: 600; color: #2196F3; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}
        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #eee; }}
        th {{ background: #f0f0f0; font-size: 0.85em; color: #666; text-transform: uppercase; }}
        tr:last-child td {{ border-bottom: none; }}
        .numeric {{ text-align: right; font-variant-numeric: tabular-nums; }}
        .empty-state {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); color: #666; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Batch Diagnostics</h1>
        <div class="last-updated">Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | <a href="index.html" style="color: #2196F3;">Back to Dashboard</a></div>
'''

if not recent_batch_rows:
    diagnostics_html += '        <div class="empty-state">No diagnostic data available yet.</div>\n'
else:
    event_order = {'submitted': 0, 'skipped': 1}
    for batch in recent_batch_rows:
        batch_data = diagnostics_by_batch.get(batch['id'], {'events': {}, 'summary': None, 'meta': batch})
        meta = batch_data.get('meta', batch)
        totals = {}
        if batch_data.get('summary'):
            totals = batch_data['summary'].get('totals', {}) or {}
        summary_items = []
        preferred_keys = [
            ('examined', 'Examined'),
            ('submitted', 'Submitted'),
            ('already_processed', 'Already Processed'),
            ('missing_title', 'Missing Title'),
            ('missing_metadata', 'Missing Metadata')
        ]
        seen_keys = set()
        for key, label in preferred_keys:
            if key in totals or key in {'examined', 'submitted'}:
                summary_items.append((label, int(totals.get(key, 0))))
                seen_keys.add(key)
        for key in sorted(totals.keys()):
            if key not in seen_keys:
                summary_items.append((humanize(key), int(totals[key])))
        diagnostics_html += f'        <div class="batch-card">\n'
        diagnostics_html += f'            <h2>Batch {batch["id"]}</h2>\n'
        diagnostics_html += '            <div class="batch-meta">'
        diagnostics_html += f'Created: {format_timestamp(meta.get("when_created"))}'
        diagnostics_html += f' &bull; Sent: {format_timestamp(meta.get("when_sent"))}'
        diagnostics_html += f' &bull; Retrieved: {format_timestamp(meta.get("when_retrieved"))}</div>\n'
        if summary_items:
            diagnostics_html += '            <div class="metrics">\n'
            for label, value in summary_items:
                diagnostics_html += '                <div class="metric">\n'
                diagnostics_html += f'                    <div class="metric-label">{label}</div>\n'
                diagnostics_html += f'                    <div class="metric-value">{value:,}</div>\n'
                diagnostics_html += '                </div>\n'
            diagnostics_html += '            </div>\n'
        event_entries = list(batch_data['events'].values())
        event_entries.sort(key=lambda e: (event_order.get(e['event_type'], 99), e['reason'] or ''))
        if event_entries:
            diagnostics_html += '            <table>\n'
            diagnostics_html += '                <thead>\n'
            diagnostics_html += '                    <tr><th>Event</th><th>Reason</th><th class="numeric">Count</th><th>Example Article IDs</th></tr>\n'
            diagnostics_html += '                </thead>\n'
            diagnostics_html += '                <tbody>\n'
            for entry in event_entries:
                event_label = humanize(entry['event_type'])
                reason_label = humanize(entry['reason'])
                sample_text = ', '.join(str(a) for a in entry['sample_article_ids']) if entry['sample_article_ids'] else '—'
                diagnostics_html += f'                    <tr><td>{event_label}</td><td>{reason_label}</td><td class="numeric">{entry["count"]:,}</td><td>{sample_text}</td></tr>\n'
            diagnostics_html += '                </tbody>\n'
            diagnostics_html += '            </table>\n'
        else:
            diagnostics_html += '            <div class="empty-state">No per-article diagnostics recorded for this batch.</div>\n'
        diagnostics_html += '        </div>\n'

diagnostics_html += '    </div>\n</body>\n</html>\n'

diagnostics_output_path = os.path.join(args.output_dir, 'diagnostics.html')
with open(diagnostics_output_path, 'w') as f:
    f.write(diagnostics_html)

# Calculate runtime
runtime_seconds = time.time() - start_time
print(f"Dashboard generated at {output_path}")
print(f"Journals page generated at {journals_output_path}")
print(f"Token usage page generated at {tokens_output_path}")
print(f"Batch diagnostics page generated at {diagnostics_output_path}")
print(f"Script runtime: {runtime_seconds:.2f} seconds")
if args.explain_queries:
    print(f"Query explanations written to {args.explain_log}")

# Close connection
cursor.close()
conn.close()
