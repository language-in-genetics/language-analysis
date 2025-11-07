#!/usr/bin/env python3
"""
Reproducibly randomly select papers from the focused journals.

Uses a fixed random seed to ensure the same papers are selected each time.
Outputs selected papers to CSV for analysis.
"""

import argparse
import os
import sys
import psycopg2
import psycopg2.extras
import csv


def get_db_connection():
    """Create a database connection using environment variables."""
    return psycopg2.connect(
        dbname=os.environ.get('PGDATABASE', 'crossref'),
        user=os.environ.get('PGUSER'),
        password=os.environ.get('PGPASSWORD'),
        host=os.environ.get('PGHOST'),
        port=os.environ.get('PGPORT', '5432')
    )


def random_sample(
    sample_size=500,
    seed=42,
    processed_only=False,
    min_year=None,
    max_year=None,
    journal=None,
    output_file=None
):
    """
    Randomly sample papers from the focused journals view.

    Args:
        sample_size: Number of papers to select
        seed: Random seed for reproducibility
        processed_only: Only select processed papers
        min_year: Minimum publication year (inclusive)
        max_year: Maximum publication year (inclusive)
        journal: Specific journal name to filter by
        output_file: CSV file to write results to

    Returns:
        List of selected papers
    """
    import random as py_random

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Build more efficient query to get candidate article IDs
    # Query raw tables directly rather than through the view for better performance
    where_conditions = ["j.enabled = true"]
    params = []

    if processed_only:
        where_conditions.append("f.processed = true")

    if min_year is not None:
        where_conditions.append("((regexp_replace(regexp_replace(r.filesrc, E'\\n', ' ', 'g'), E'\\t', '    ', 'g')::jsonb -> 'published' -> 'date-parts' -> 0 ->> 0)::integer) >= %s")
        params.append(min_year)

    if max_year is not None:
        where_conditions.append("((regexp_replace(regexp_replace(r.filesrc, E'\\n', ' ', 'g'), E'\\t', '    ', 'g')::jsonb -> 'published' -> 'date-parts' -> 0 ->> 0)::integer) <= %s")
        params.append(max_year)

    if journal:
        where_conditions.append("j.name = %s")
        params.append(journal)

    where_clause = "WHERE " + " AND ".join(where_conditions)

    # Get all matching article IDs (lightweight query)
    # IMPORTANT: ORDER BY ensures deterministic order for reproducible sampling
    print(f"Finding candidate articles (seed={seed})...", file=sys.stderr)
    id_query = f"""
        SELECT DISTINCT r.id
        FROM public.raw_text_data r
        INNER JOIN languageingenetics.journals j
            ON (regexp_replace(regexp_replace(r.filesrc, E'\\n', ' ', 'g'), E'\\t', '    ', 'g')::jsonb -> 'container-title' ->> 0) = j.name
        LEFT JOIN languageingenetics.files f ON r.id = f.article_id
        {where_clause}
        ORDER BY r.id
    """

    cur.execute(id_query, params)
    all_ids = [row['id'] for row in cur.fetchall()]

    if not all_ids:
        print("No matching articles found", file=sys.stderr)
        cur.close()
        conn.close()
        return []

    print(f"Found {len(all_ids)} candidate articles", file=sys.stderr)

    # Use Python's random to sample (reproducible with seed)
    py_random.seed(seed)
    selected_ids = py_random.sample(all_ids, min(sample_size, len(all_ids)))
    print(f"Randomly selected {len(selected_ids)} articles", file=sys.stderr)

    # Now fetch full details for just the selected IDs from the view
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
        description='Reproducibly randomly sample papers from focused journals',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Select 500 random papers with default seed (42)
  %(prog)s --output sample_500.csv

  # Select 100 papers with custom seed
  %(prog)s --sample-size 100 --seed 12345 --output sample_100.csv

  # Select only processed papers from 2015-2020
  %(prog)s --processed-only --min-year 2015 --max-year 2020 --output recent_processed.csv

  # Select from specific journal
  %(prog)s --journal "The American Journal of Human Genetics" --output ajhg_sample.csv

  # Different seed gives different sample
  %(prog)s --seed 99 --output different_sample.csv
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
        '--processed-only', '-p',
        action='store_true',
        help='Only select papers that have been processed by OpenAI'
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
        help='Output CSV file path (if not specified, just prints stats)'
    )

    parser.add_argument(
        '--list-journals',
        action='store_true',
        help='List available journals and exit'
    )

    args = parser.parse_args()

    # List journals if requested
    if args.list_journals:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT journal_name
            FROM languageingenetics.focused_journals_view
            ORDER BY journal_name
        """)
        print("Available journals in focused_journals_view:")
        for row in cur.fetchall():
            print(f"  - {row[0]}")
        cur.close()
        conn.close()
        return

    # Perform random sampling
    papers = random_sample(
        sample_size=args.sample_size,
        seed=args.seed,
        processed_only=args.processed_only,
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
        for j, count in sorted(journals.items(), key=lambda x: x[1], reverse=True):
            print(f"    {j}: {count}", file=sys.stderr)

        # Count processed
        processed = sum(1 for p in papers if p['is_processed'])
        print(f"  Processed: {processed} ({100*processed/len(papers):.1f}%)", file=sys.stderr)

        # Year range
        years = [p['pub_year'] for p in papers if p['pub_year']]
        if years:
            print(f"  Year range: {min(years)}-{max(years)}", file=sys.stderr)

        # Terminology stats (if processed)
        if processed > 0:
            caucasian = sum(1 for p in papers if p['caucasian'])
            white = sum(1 for p in papers if p['white'])
            european = sum(1 for p in papers if p['european'])
            other = sum(1 for p in papers if p['other'])

            print(f"  Terminology usage (processed papers only):", file=sys.stderr)
            print(f"    Caucasian: {caucasian} ({100*caucasian/processed:.1f}%)", file=sys.stderr)
            print(f"    White: {white} ({100*white/processed:.1f}%)", file=sys.stderr)
            print(f"    European: {european} ({100*european/processed:.1f}%)", file=sys.stderr)
            print(f"    Other: {other} ({100*other/processed:.1f}%)", file=sys.stderr)


if __name__ == '__main__':
    main()
