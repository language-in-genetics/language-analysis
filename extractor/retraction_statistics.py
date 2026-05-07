#!/usr/bin/env python3

import argparse
import json
import os
import sys

import psycopg2
import psycopg2.extras

from retraction_stats import (
    PROCESSED_ARTICLES_SQL,
    PROCESSED_FILES_SQL,
    build_retraction_statistics,
    build_retraction_statistics_from_work_ids,
    format_p_value,
    format_rate,
    load_retraction_status_from_jsonl_gz,
    resolve_status_work_ids,
    write_stats_csv,
)


def print_summary(stats):
    population = stats["population"]
    print("Retraction vocabulary statistics")
    print(f"  Processed focused articles: {population['processed_focused_articles']:,}")
    print(f"  Eligible test articles:     {population['eligible_articles']:,}")
    print(f"  Retracted articles:         {population['retracted_articles']:,}")
    print(f"  Non-retracted controls:     {population['non_retracted_articles']:,}")
    print(f"  Excluded retraction notices:{population['excluded_retraction_notices']:,}")
    print()
    print("Outcome\tRetracted rate\tControl rate\tFisher p\tOdds ratio")
    for test in stats["tests"]:
        odds_ratio = test["odds_ratio_haldane"]
        odds_text = "N/A" if odds_ratio is None else f"{odds_ratio:.3f}"
        print(
            "\t".join([
                test["label"],
                format_rate(test["retracted_rate"]),
                format_rate(test["non_retracted_rate"]),
                format_p_value(test["fisher_exact_p"]),
                odds_text,
            ])
        )


def main():
    parser = argparse.ArgumentParser(
        description="Test whether retracted focused-journal articles differ in race-language vocabulary usage."
    )
    parser.add_argument("--output-json", help="Write full statistics to this JSON file")
    parser.add_argument("--output-csv", help="Write test table to this CSV file")
    parser.add_argument(
        "--source-jsonl-gz",
        default=os.environ.get("CROSSREF_RETRACTION_SOURCE_JSONL_GZ"),
        help="Focused Crossref JSONL gzip to use as the retraction-status source",
    )
    parser.add_argument("--list-retracted", action="store_true", help="Print detected retracted article examples")
    args = parser.parse_args()

    conn = psycopg2.connect("")
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("SET search_path TO languageingenetics, public")
    cursor.execute("SET enable_hashjoin = off")
    cursor.execute("SET enable_mergejoin = off")
    if args.source_jsonl_gz:
        status = load_retraction_status_from_jsonl_gz(args.source_jsonl_gz)
        status_work_ids = resolve_status_work_ids(cursor, status)
        cursor.execute(PROCESSED_FILES_SQL)
        stats = build_retraction_statistics_from_work_ids(cursor.fetchall(), status_work_ids)
    else:
        cursor.execute(PROCESSED_ARTICLES_SQL)
        stats = build_retraction_statistics(cursor.fetchall())
    cursor.close()
    conn.close()

    if args.output_json:
        with open(args.output_json, "w") as output:
            json.dump(stats, output, indent=2)
            output.write("\n")

    if args.output_csv:
        write_stats_csv(stats, args.output_csv)

    print_summary(stats)

    if args.list_retracted:
        print("\nDetected retracted articles:", file=sys.stderr)
        for item in stats["retracted_examples"]:
            print(
                f"- {item.get('pub_year') or '?'} {item.get('journal') or '?'} "
                f"{item.get('doi') or '?'}: {item.get('title') or ''}",
                file=sys.stderr,
            )


if __name__ == "__main__":
    main()
