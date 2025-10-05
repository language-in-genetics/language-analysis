#!/usr/bin/env python3

import argparse
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import json
import os
import time

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

# Helper function to execute queries with optional EXPLAIN logging
def execute_query(sql, params=None):
    """Execute a query, optionally logging EXPLAIN output"""
    if args.explain_queries:
        explain_cursor = conn.cursor()
        try:
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
        finally:
            explain_cursor.close()

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

# Journal statistics
journal_stats = []
for journal in enabled_journals:
    execute_query("""
        SELECT COUNT(*) as total
        FROM public.raw_text_data
        WHERE (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title' @> %s::jsonb)
    """, [json.dumps([journal])])
    total = cursor.fetchone()['total']

    if total > 0:
        # Get processed count
        execute_query("""
            SELECT COUNT(*) as processed
            FROM languageingenetics.files f
            JOIN public.raw_text_data r ON f.article_id = r.id
            WHERE f.processed = true
            AND (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title' @> %s::jsonb)
        """, [json.dumps([journal])])
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

# Results by year
execute_query("""
    SELECT
        pub_year as year,
        COUNT(*) as count
    FROM languageingenetics.files
    WHERE processed = true
    AND (caucasian = true OR white = true OR european = true OR other = true)
    AND pub_year IS NOT NULL
    GROUP BY pub_year
    ORDER BY pub_year
""")
by_year = [{'year': row['year'], 'count': row['count']} for row in cursor.fetchall()]

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
        <div class="last-updated">Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | <a href="journals.html" style="color: #2196F3;">View All Genetics Journals</a></div>

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
        const byYearData = """ + json.dumps(by_year) + """;
        const byJournalYearData = """ + json.dumps(by_journal_year_final) + """;

        // Year chart
        const yearCtx = document.getElementById('yearChart').getContext('2d');
        new Chart(yearCtx, {
            type: 'line',
            data: {
                labels: byYearData.map(d => d.year),
                datasets: [{
                    label: 'Articles with Race References',
                    data: byYearData.map(d => d.count),
                    borderColor: '#2196F3',
                    backgroundColor: 'rgba(33, 150, 243, 0.1)',
                    tension: 0.1,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: true }
                },
                scales: {
                    y: { beginAtZero: true, title: { display: true, text: 'Count' } },
                    x: { title: { display: true, text: 'Year' } }
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

# Query all journals with "Genetic" in their title and count their articles
execute_query("""
    WITH journal_titles AS (
        SELECT
            regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title'->>0 AS journal_name
        FROM public.raw_text_data
    )
    SELECT
        journal_name,
        COUNT(*) AS article_count
    FROM journal_titles
    WHERE journal_name ILIKE '%genetic%'
        AND journal_name IS NOT NULL
        AND journal_name <> ''
    GROUP BY journal_name
    HAVING COUNT(*) >= 10
    ORDER BY article_count DESC, journal_name
""")
genetic_journals_raw = cursor.fetchall()

# Get all journals from the journals table
execute_query("SELECT name, enabled FROM languageingenetics.journals ORDER BY name")
tracked_journals = {row['name']: row['enabled'] for row in cursor.fetchall()}

# Build the data structure for the journals page
journals_data = []
for row in genetic_journals_raw:
    journal_name = row['journal_name'].strip() if row['journal_name'] else row['journal_name']
    article_count = row['article_count']

    # Check if this journal is tracked
    if journal_name in tracked_journals:
        status = "Active" if tracked_journals[journal_name] else "Inactive"
        tracked = True
    else:
        status = "Not Tracked"
        tracked = False

    journals_data.append({
        'name': journal_name,
        'tracked': tracked,
        'status': status,
        'article_count': article_count
    })

# Generate journals HTML page
journals_html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Genetics Journals - Word Frequency Analysis</title>
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
        .last-updated {{ color: #999; font-size: 0.9em; margin-bottom: 30px; }}
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
        .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-top: 15px; }}
        .summary-item {{ text-align: center; }}
        .summary-value {{ font-size: 2em; font-weight: bold; color: #2196F3; }}
        .summary-label {{ color: #666; font-size: 0.9em; margin-top: 5px; }}
        .numeric {{ text-align: right; font-variant-numeric: tabular-nums; color: #333; }}
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
                    <div class="summary-label">Journals with "Genetic" (≥10 articles)</div>
                </div>
                <div class="summary-item">
                    <div class="summary-value">{len([j for j in journals_data if j['tracked']])}</div>
                    <div class="summary-label">Tracked Journals</div>
                </div>
                <div class="summary-item">
                    <div class="summary-value">{len([j for j in journals_data if j['status'] == 'Active'])}</div>
                    <div class="summary-label">Active Journals</div>
                </div>
                <div class="summary-item">
                    <div class="summary-value">{len([j for j in journals_data if j['status'] == 'Not Tracked'])}</div>
                    <div class="summary-label">Not Tracked</div>
                </div>
                <div class="summary-item">
                    <div class="summary-value">{sum(j['article_count'] for j in journals_data):,}</div>
                    <div class="summary-label">Total Articles Across Journals</div>
                </div>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th>Journal Name</th>
                    <th>Articles</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
"""

for journal in journals_data:
    status_class = f"status-{journal['status'].lower().replace(' ', '-')}"
    journals_html += f"""
                <tr>
                    <td>{journal['name']}</td>
                    <td class="numeric">{journal['article_count']:,}</td>
                    <td class="{status_class}">{journal['status']}</td>
                </tr>
"""

journals_html += """
            </tbody>
        </table>
    </div>
</body>
</html>
"""

# Write journals HTML file
journals_output_path = os.path.join(args.output_dir, 'journals.html')
with open(journals_output_path, 'w') as f:
    f.write(journals_html)

# Calculate runtime
runtime_seconds = time.time() - start_time
print(f"Dashboard generated at {output_path}")
print(f"Journals page generated at {journals_output_path}")
print(f"Script runtime: {runtime_seconds:.2f} seconds")
if args.explain_queries:
    print(f"Query explanations written to {args.explain_log}")

# Close connection
cursor.close()
conn.close()
