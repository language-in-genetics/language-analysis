#!/usr/bin/env python3

import argparse
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import json
import os

parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", default="dashboard", help="Output directory for static files")
args = parser.parse_args()

# Create output directory
os.makedirs(args.output_dir, exist_ok=True)

# Database connection using environment variables (PGDATABASE, PGHOST, etc.)
conn = psycopg2.connect("")
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Set search path
cursor.execute("SET search_path TO languageingenetics, public")

print("Collecting data...")

# Progress data
cursor.execute("SELECT COUNT(*) FROM public.raw_text_data")
total_articles = cursor.fetchone()['count']

cursor.execute("SELECT COUNT(*) FROM languageingenetics.files WHERE processed = true")
processed_articles = cursor.fetchone()['count']

progress_data = {
    'total_articles': total_articles,
    'processed_articles': processed_articles,
    'unprocessed_articles': total_articles - processed_articles,
    'processing_percentage': (processed_articles / total_articles * 100) if total_articles > 0 else 0
}

# Token usage data
cursor.execute("""
    SELECT
        COALESCE(SUM(prompt_tokens), 0) as prompt_tokens,
        COALESCE(SUM(completion_tokens), 0) as completion_tokens
    FROM languageingenetics.files
    WHERE processed = true
""")
row = cursor.fetchone()
all_time_prompt = row['prompt_tokens']
all_time_completion = row['completion_tokens']

today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
cursor.execute("""
    SELECT
        COALESCE(SUM(prompt_tokens), 0) as prompt_tokens,
        COALESCE(SUM(completion_tokens), 0) as completion_tokens
    FROM languageingenetics.files
    WHERE processed = true AND when_processed >= %s
""", [today_start])
row = cursor.fetchone()
today_prompt = row['prompt_tokens']
today_completion = row['completion_tokens']

last_24h = datetime.now() - timedelta(hours=24)
cursor.execute("""
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
    'today': {
        'prompt_tokens': today_prompt,
        'completion_tokens': today_completion,
        'total_tokens': today_prompt + today_completion
    },
    'last_24h': {
        'prompt_tokens': last24h_prompt,
        'completion_tokens': last24h_completion,
        'total_tokens': last24h_prompt + last24h_completion
    }
}

# Journal statistics
cursor.execute("SELECT name FROM languageingenetics.journals WHERE enabled = true ORDER BY name")
enabled_journals = [row['name'] for row in cursor]

journal_stats = []
for journal in enabled_journals:
    cursor.execute("""
        SELECT COUNT(*) as total
        FROM public.raw_text_data
        WHERE (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title' @> %s::jsonb)
    """, [json.dumps([journal])])
    total = cursor.fetchone()['total']

    if total > 0:
        # Get processed count
        cursor.execute("""
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

# Results by year
cursor.execute("""
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
cursor.execute("""
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
        <div class="last-updated">Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>

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
                <h3>Active Journals</h3>
                <div class="value">{len([j for j in journal_stats if j['processed'] > 0])}</div>
                <div class="subvalue">of {len(enabled_journals)} enabled</div>
            </div>
        </div>

        <h2>Token Usage</h2>
        <div class="grid">
            <div class="card">
                <h3>All Time</h3>
                <div class="value">${(token_data['all_time']['prompt_tokens'] * 0.075 / 1000000 + token_data['all_time']['completion_tokens'] * 0.3 / 1000000):.2f}</div>
                <div class="subvalue">{token_data['all_time']['total_tokens']:,} tokens</div>
            </div>
            <div class="card">
                <h3>Today</h3>
                <div class="value">${(token_data['today']['prompt_tokens'] * 0.075 / 1000000 + token_data['today']['completion_tokens'] * 0.3 / 1000000):.2f}</div>
                <div class="subvalue">{token_data['today']['total_tokens']:,} tokens</div>
            </div>
            <div class="card">
                <h3>Last 24 Hours</h3>
                <div class="value">${(token_data['last_24h']['prompt_tokens'] * 0.075 / 1000000 + token_data['last_24h']['completion_tokens'] * 0.3 / 1000000):.2f}</div>
                <div class="subvalue">{token_data['last_24h']['total_tokens']:,} tokens</div>
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

html_content += """
            </tbody>
        </table>

        <h2>References by Year</h2>
        <div class="chart-container">
            <canvas id="yearChart"></canvas>
        </div>

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

print(f"Dashboard generated at {output_path}")

# Close connection
cursor.close()
conn.close()
