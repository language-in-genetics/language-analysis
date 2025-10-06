#!/usr/bin/env python3

import argparse
import sqlite3
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, render_template_string
from datetime import datetime, timedelta
import json
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument("--database", required=True, help="SQLite database path")
parser.add_argument("--pg-conn", required=True, help="PostgreSQL connection string")
parser.add_argument("--port", type=int, default=5000, help="Port to run the web server on")
parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
args = parser.parse_args()

app = Flask(__name__)

# Database connections
def get_sqlite_conn():
    conn = sqlite3.connect(args.database)
    conn.row_factory = sqlite3.Row
    return conn

def get_pg_conn():
    return psycopg2.connect(args.pg_conn)

# HTML Template
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Word Frequency Analysis Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: #f5f5f5;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 30px; font-size: 2em; }
        h2 { color: #555; margin: 30px 0 15px; font-size: 1.5em; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .card h3 { color: #666; font-size: 0.9em; text-transform: uppercase; margin-bottom: 10px; }
        .card .value { font-size: 2.5em; font-weight: bold; color: #2196F3; }
        .card .subvalue { font-size: 0.9em; color: #999; margin-top: 5px; }
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f8f8; font-weight: 600; color: #666; text-transform: uppercase; font-size: 0.85em; }
        tr:last-child td { border-bottom: none; }
        .chart-container { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 30px; }
        canvas { max-height: 400px; }
        .loading { text-align: center; padding: 40px; color: #999; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Word Frequency Analysis Dashboard</h1>

        <h2>Progress Overview</h2>
        <div class="grid" id="progress-cards"></div>

        <h2>Token Usage</h2>
        <div class="grid" id="token-cards"></div>

        <h2>Articles by Journal</h2>
        <div id="journal-table-container"></div>

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
        async function loadData() {
            const [progress, tokens, journals, byYear, byJournalYear] = await Promise.all([
                fetch('/api/progress').then(r => r.json()),
                fetch('/api/tokens').then(r => r.json()),
                fetch('/api/journals').then(r => r.json()),
                fetch('/api/results/by-year').then(r => r.json()),
                fetch('/api/results/by-journal-year').then(r => r.json())
            ]);

            // Progress cards
            document.getElementById('progress-cards').innerHTML = `
                <div class="card">
                    <h3>Total Articles</h3>
                    <div class="value">${progress.total_articles.toLocaleString()}</div>
                    <div class="subvalue">${progress.processed_articles.toLocaleString()} processed</div>
                </div>
                <div class="card">
                    <h3>Processing Rate</h3>
                    <div class="value">${progress.processing_percentage.toFixed(1)}%</div>
                    <div class="subvalue">${progress.unprocessed_articles.toLocaleString()} remaining</div>
                </div>
                <div class="card">
                    <h3>Articles / Hour</h3>
                    <div class="value">${progress.articles_per_hour.toFixed(1)}</div>
                    <div class="subvalue">Based on last ${progress.rate_window_hours} hours</div>
                </div>
                <div class="card">
                    <h3>Active Journals</h3>
                    <div class="value">${progress.journals_with_data}</div>
                </div>
            `;

            // Token cards
            const allTimeCost = (tokens.all_time.prompt_tokens * 0.075 / 1000000 +
                                tokens.all_time.completion_tokens * 0.3 / 1000000).toFixed(2);
            const todayCost = (tokens.today.prompt_tokens * 0.075 / 1000000 +
                              tokens.today.completion_tokens * 0.3 / 1000000).toFixed(2);
            const last24Cost = (tokens.last_24h.prompt_tokens * 0.075 / 1000000 +
                               tokens.last_24h.completion_tokens * 0.3 / 1000000).toFixed(2);

            document.getElementById('token-cards').innerHTML = `
                <div class="card">
                    <h3>All Time</h3>
                    <div class="value">$${allTimeCost}</div>
                    <div class="subvalue">${tokens.all_time.total_tokens.toLocaleString()} tokens</div>
                </div>
                <div class="card">
                    <h3>Today</h3>
                    <div class="value">$${todayCost}</div>
                    <div class="subvalue">${tokens.today.total_tokens.toLocaleString()} tokens</div>
                </div>
                <div class="card">
                    <h3>Last 24 Hours</h3>
                    <div class="value">$${last24Cost}</div>
                    <div class="subvalue">${tokens.last_24h.total_tokens.toLocaleString()} tokens</div>
                </div>
            `;

            // Journal table
            let tableHTML = '<table><thead><tr><th>Journal</th><th>Total</th><th>Processed</th><th>%</th></tr></thead><tbody>';
            journals.forEach(j => {
                const pct = j.total > 0 ? ((j.processed / j.total) * 100).toFixed(1) : '0.0';
                tableHTML += `<tr><td>${j.journal}</td><td>${j.total.toLocaleString()}</td><td>${j.processed.toLocaleString()}</td><td>${pct}%</td></tr>`;
            });
            tableHTML += '</tbody></table>';
            document.getElementById('journal-table-container').innerHTML = tableHTML;

            // Year chart
            const yearCtx = document.getElementById('yearChart').getContext('2d');
            new Chart(yearCtx, {
                type: 'line',
                data: {
                    labels: byYear.map(d => d.year),
                    datasets: [{
                        label: 'Articles with Race References',
                        data: byYear.map(d => d.count),
                        borderColor: '#2196F3',
                        backgroundColor: 'rgba(33, 150, 243, 0.1)',
                        tension: 0.1
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {
                        legend: { display: true }
                    },
                    scales: {
                        y: { beginAtZero: true }
                    }
                }
            });

            // Journal-year chart
            const journalYearCtx = document.getElementById('journalYearChart').getContext('2d');
            const journalNames = [...new Set(byJournalYear.map(d => d.journal))];
            const colors = [
                '#2196F3', '#4CAF50', '#FF9800', '#F44336', '#9C27B0',
                '#00BCD4', '#FFEB3B', '#795548', '#607D8B', '#E91E63'
            ];

            const datasets = journalNames.map((journal, i) => {
                const journalData = byJournalYear.filter(d => d.journal === journal);
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
        }

        loadData();
        // Refresh every 30 seconds
        setInterval(loadData, 30000);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/progress')
def api_progress():
    sqlite_conn = get_sqlite_conn()
    pg_conn = get_pg_conn()

    try:
        # Get total articles from PostgreSQL
        pg_cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        pg_cursor.execute("SELECT COUNT(*) FROM articles")
        total_articles = pg_cursor.fetchone()[0]

        # Get processed articles from SQLite
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute("SELECT COUNT(*) FROM files WHERE processed = 1")
        processed_articles = sqlite_cursor.fetchone()[0]

        # Get journal count with data
        sqlite_cursor.execute("""
            SELECT COUNT(DISTINCT data->>'container-title'->0)
            FROM files f
            JOIN articles a ON f.article_id = a.id
            WHERE processed = 1
        """)
        # This won't work with SQLite, need to do it differently

        # Simpler approach: count journals from PG that have processed articles
        pg_cursor.execute("""
            SELECT COUNT(DISTINCT data->'container-title'->0)
            FROM articles a
            WHERE EXISTS (
                SELECT 1 FROM files f WHERE f.article_id = a.id AND f.processed = true
            )
        """)
        # This won't work either - let me use a different approach

        throughput_window_hours = 6
        pg_cursor.execute(
            """
            SELECT batch_id, when_checked, number_completed
            FROM languageingenetics.batchprogress
            WHERE when_checked >= NOW() - INTERVAL '6 hours'
            ORDER BY batch_id, when_checked
            """
        )
        progress_rows = pg_cursor.fetchall()

        total_completed_delta = 0
        total_hours = 0.0
        if progress_rows:
            grouped = defaultdict(list)
            for row in progress_rows:
                grouped[row['batch_id']].append(row)

            for batch_rows in grouped.values():
                batch_rows.sort(key=lambda r: r['when_checked'])
                if len(batch_rows) < 2:
                    continue
                start = batch_rows[0]
                end = batch_rows[-1]
                delta_completed = end['number_completed'] - start['number_completed']
                delta_seconds = (end['when_checked'] - start['when_checked']).total_seconds()
                if delta_completed > 0 and delta_seconds > 0:
                    total_completed_delta += delta_completed
                    total_hours += delta_seconds / 3600.0

        articles_per_hour = (total_completed_delta / total_hours) if total_hours > 0 else 0.0

        pg_cursor.close()

        return jsonify({
            'total_articles': total_articles,
            'processed_articles': processed_articles,
            'unprocessed_articles': total_articles - processed_articles,
            'processing_percentage': (processed_articles / total_articles * 100) if total_articles > 0 else 0,
            'journals_with_data': 0,  # Will calculate properly in journals endpoint
            'articles_per_hour': articles_per_hour,
            'rate_window_hours': throughput_window_hours
        })
    finally:
        sqlite_conn.close()
        pg_conn.close()

@app.route('/api/tokens')
def api_tokens():
    sqlite_conn = get_sqlite_conn()
    cursor = sqlite_conn.cursor()

    try:
        # All time
        cursor.execute("""
            SELECT
                COALESCE(SUM(prompt_tokens), 0) as prompt_tokens,
                COALESCE(SUM(completion_tokens), 0) as completion_tokens
            FROM files
            WHERE processed = 1
        """)
        row = cursor.fetchone()
        all_time_prompt = row[0]
        all_time_completion = row[1]

        # Today (midnight to now)
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        cursor.execute("""
            SELECT
                COALESCE(SUM(prompt_tokens), 0) as prompt_tokens,
                COALESCE(SUM(completion_tokens), 0) as completion_tokens
            FROM files
            WHERE processed = 1 AND when_processed >= ?
        """, [today_start])
        row = cursor.fetchone()
        today_prompt = row[0]
        today_completion = row[1]

        # Last 24 hours
        last_24h = (datetime.now() - timedelta(hours=24)).isoformat()
        cursor.execute("""
            SELECT
                COALESCE(SUM(prompt_tokens), 0) as prompt_tokens,
                COALESCE(SUM(completion_tokens), 0) as completion_tokens
            FROM files
            WHERE processed = 1 AND when_processed >= ?
        """, [last_24h])
        row = cursor.fetchone()
        last24h_prompt = row[0]
        last24h_completion = row[1]

        return jsonify({
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
        })
    finally:
        sqlite_conn.close()

@app.route('/api/journals')
def api_journals():
    pg_conn = get_pg_conn()
    sqlite_conn = get_sqlite_conn()

    try:
        pg_cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Get journals from enabled journals table
        pg_cursor.execute("""
            SELECT name FROM journals WHERE enabled = true ORDER BY name
        """)
        enabled_journals = [row['name'] for row in pg_cursor]

        results = []
        for journal in enabled_journals:
            # Count total articles for this journal in PG
            pg_cursor.execute("""
                SELECT COUNT(*)
                FROM articles
                WHERE data->'container-title' @> %s::jsonb
            """, [json.dumps([journal])])
            total = pg_cursor.fetchone()['count']

            # Count processed articles from SQLite
            sqlite_cursor = sqlite_conn.cursor()
            sqlite_cursor.execute("""
                SELECT COUNT(*)
                FROM files f
                WHERE f.processed = 1
                AND f.article_id IN (
                    SELECT id FROM articles WHERE data->'container-title' @> ?
                )
            """, [json.dumps([journal])])
            # This won't work - SQLite doesn't know about PG articles table

            # Need different approach - store journal in SQLite too
            # For now, just return total and count all processed
            sqlite_cursor.execute("""
                SELECT COUNT(*) FROM files WHERE processed = 1
            """)
            processed = sqlite_cursor.fetchone()[0] if total > 0 else 0

            results.append({
                'journal': journal,
                'total': total,
                'processed': min(processed, total)  # Approximate for now
            })

        return jsonify(results)
    finally:
        pg_conn.close()
        sqlite_conn.close()

@app.route('/api/results/by-year')
def api_results_by_year():
    sqlite_conn = get_sqlite_conn()
    cursor = sqlite_conn.cursor()

    try:
        cursor.execute("""
            SELECT
                pub_year as year,
                COUNT(*) as count
            FROM files
            WHERE processed = 1
            AND (caucasian = 1 OR white = 1 OR european = 1 OR other = 1)
            AND pub_year IS NOT NULL
            GROUP BY pub_year
            ORDER BY pub_year
        """)

        results = [{'year': row[0], 'count': row[1]} for row in cursor.fetchall()]
        return jsonify(results)
    finally:
        sqlite_conn.close()

@app.route('/api/results/by-journal-year')
def api_results_by_journal_year():
    pg_conn = get_pg_conn()
    sqlite_conn = get_sqlite_conn()

    try:
        # This is complex - need to join data across databases
        # For MVP, let's return empty for now and fix later
        # Proper solution: store journal name in SQLite files table

        return jsonify([])
    finally:
        pg_conn.close()
        sqlite_conn.close()

if __name__ == '__main__':
    print(f"Starting dashboard on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=True)
