#!/usr/bin/env python3
"""
Quick reproducible random sampling from processed articles.

This version is optimized for speed by sampling only from already-processed
articles in the files table, which is much smaller and faster to query than
the entire raw_text_data table.
"""

import argparse
import os
import sys
import psycopg2
import psycopg2.extras
import csv
import random as py_random


def get_db_connection():
    """Create a database connection using environment variables."""
    return psycopg2.connect(
        dbname=os.environ.get('PGDATABASE', 'crossref'),
        user=os.environ.get('PGUSER'),
        password=os.environ.get('PGPASSWORD'),
        host=os.environ.get('PGHOST'),
        port=os.environ.get('PGPORT', '5432')
    )


def quick_sample(
    sample_size=500,
    seed=42,
    min_year=None,
    max_year=None,
    journal=None,
    output_file=None
):
    """
    Quickly sample from processed articles.

    Args:
        sample_size: Number of papers to select
        seed: Random seed for reproducibility
        min_year: Minimum publication year (inclusive)
        max_year: Maximum publication year (inclusive)
        journal: Specific journal name to filter by
        output_file: CSV file to write results to

    Returns:
        List of selected papers
    """
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Build WHERE clause for filtering
    where_conditions = []
    params = []

    if min_year is not None:
        where_conditions.append("pub_year >= %s")
        params.append(min_year)

    if max_year is not None:
        where_conditions.append("pub_year <= %s")
        params.append(max_year)

    if journal:
        where_conditions.append("journal_name = %s")
        params.append(journal)

    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)

    # Quick query: get article IDs from the files table (small, indexed table)
    # with year and journal info from the view
    # IMPORTANT: ORDER BY ensures deterministic order for reproducible sampling
    print(f"Finding processed articles (seed={seed})...", file=sys.stderr)
    id_query = f"""
        SELECT article_id
        FROM languageingenetics.focused_journals_view
        WHERE is_processed = true
        {('AND ' + ' AND '.join(where_conditions)) if where_conditions else ''}
        ORDER BY article_id
    """

    cur.execute(id_query, params)
    all_ids = [row['article_id'] for row in cur.fetchall()]

    if not all_ids:
        print("No matching processed articles found", file=sys.stderr)
        cur.close()
        conn.close()
        return []

    print(f"Found {len(all_ids)} processed articles", file=sys.stderr)

    # Use Python's random to sample (reproducible with seed)
    py_random.seed(seed)
    selected_ids = py_random.sample(all_ids, min(sample_size, len(all_ids)))
    print(f"Randomly selected {len(selected_ids)} articles", file=sys.stderr)

    # Fetch full details for selected articles
    query = """
        SELECT
            article_id,
            journal_name,
            doi,
            title,
            pub_year,
            abstract,
            article_type,
            is_processed,
            has_abstract,
            when_processed,
            caucasian,
            white,
            european,
            european_phrase_used,
            other,
            other_phrase_used,
            prompt_tokens,
            completion_tokens
        FROM languageingenetics.focused_journals_view
        WHERE article_id = ANY(%s)
    """

    print(f"Fetching full details...", file=sys.stderr)
    cur.execute(query, (selected_ids,))
    papers = cur.fetchall()

    print(f"Retrieved {len(papers)} papers", file=sys.stderr)

    # Write to CSV if output file specified
    if output_file and papers:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=papers[0].keys())
            writer.writeheader()
            writer.writerows(papers)
        print(f"Wrote results to {output_file}", file=sys.stderr)

    cur.close()
    conn.close()

    return papers


def main():
    parser = argparse.ArgumentParser(
        description='Quick random sampling from processed articles in focused journals',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This tool samples only from already-processed articles for speed.
For sampling from all articles (including unprocessed), use random_sample.py instead.

Examples:
  # Select 500 random processed papers
  %(prog)s --output sample_500.csv

  # Select 100 papers with custom seed
  %(prog)s --sample-size 100 --seed 12345 --output sample_100.csv

  # Select from 2015-2020
  %(prog)s --min-year 2015 --max-year 2020 --output recent.csv

  # Select from specific journal
  %(prog)s --journal "The American Journal of Human Genetics" --output ajhg_sample.csv
        """
    )

    parser.add_argument(
        '--sample-size', '-n',
        type=int,
        default=500,
        help='Number of papers to select (default: 500)'
    )

    parser.add_argument(
        '--seed', '-s',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )

    parser.add_argument(
        '--min-year',
        type=int,
        help='Minimum publication year (inclusive)'
    )

    parser.add_argument(
        '--max-year',
        type=int,
        help='Maximum publication year (inclusive)'
    )

    parser.add_argument(
        '--journal', '-j',
        type=str,
        help='Filter to specific journal name'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output CSV file path'
    )

    args = parser.parse_args()

    # Perform sampling
    papers = quick_sample(
        sample_size=args.sample_size,
        seed=args.seed,
        min_year=args.min_year,
        max_year=args.max_year,
        journal=args.journal,
        output_file=args.output
    )

    # Print summary statistics
    if papers:
        print("\nSummary:", file=sys.stderr)
        print(f"  Total papers: {len(papers)}", file=sys.stderr)

        # Count by journal
        journals = {}
        for p in papers:
            j = p['journal_name']
            journals[j] = journals.get(j, 0) + 1

        print(f"  Journals represented: {len(journals)}", file=sys.stderr)
        for j, count in sorted(journals.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"    {j}: {count}", file=sys.stderr)

        # Year range
        years = [p['pub_year'] for p in papers if p['pub_year']]
        if years:
            print(f"  Year range: {min(years)}-{max(years)}", file=sys.stderr)

        # Terminology stats
        caucasian = sum(1 for p in papers if p['caucasian'])
        white = sum(1 for p in papers if p['white'])
        european = sum(1 for p in papers if p['european'])
        other = sum(1 for p in papers if p['other'])

        print(f"  Terminology usage:", file=sys.stderr)
        print(f"    Caucasian: {caucasian} ({100*caucasian/len(papers):.1f}%)", file=sys.stderr)
        print(f"    White: {white} ({100*white/len(papers):.1f}%)", file=sys.stderr)
        print(f"    European: {european} ({100*european/len(papers):.1f}%)", file=sys.stderr)
        print(f"    Other: {other} ({100*other/len(papers):.1f}%)", file=sys.stderr)


if __name__ == '__main__':
    main()
