#!/usr/bin/env python3

import argparse
import json
import os

import psycopg2
import psycopg2.extras

from author_persistence_stats import (
    AUTHOR_SOURCE_SQL,
    DEFAULT_PEAK_YEAR,
    DEFAULT_RECENT_START_YEAR,
    build_author_persistence_statistics,
    format_p_value,
    format_rate,
    load_author_source_rows_from_sqlite,
    write_author_persistence_annual_csv,
    write_author_persistence_csv,
    write_author_persistence_html,
)


def print_summary(stats):
    population = stats["population"]
    cohort = stats["cohort"]
    behavior = cohort["active_pre_caucasian_recent_behavior"]
    print("Shared-author Caucasian persistence")
    print(f"  Analyzed articles:              {population['analyzed_articles']:,}")
    print(f"  Articles with matched authors:  {population['articles_with_matched_author']:,}")
    print(f"  Matched author identities:      {population['author_identities']:,}")
    print(f"  Authors ever using Caucasian:   {population['author_identities_with_any_caucasian']:,}")
    print(f"  Recent period starts:           {cohort['recent_start_year']}")
    print(f"  Pre-recent Caucasian authors:   {cohort['pre_caucasian_authors']:,}")
    print(f"  Active previous users:          {behavior['authors']:,}")
    decomposition = stats["legacy_reduction_decomposition"]
    print(f"  Peak year for decomposition:    {decomposition['peak_year']}")
    print(
        "  Reduction behavior component: "
        f"{(decomposition['behavior_reduction_share'] or 0) * 100:.1f}%"
    )
    print(
        "  Reduction composition component: "
        f"{(decomposition['composition_reduction_share'] or 0) * 100:.1f}%"
    )
    print()
    print("Test\tUnit\tExposed rate\tComparison rate\tFisher p\tOdds ratio")
    for test in list(stats["predictive_tests"]) + list(cohort["tests"]):
        odds_ratio = test["odds_ratio_haldane"]
        odds_text = "N/A" if odds_ratio is None else f"{odds_ratio:.3f}"
        print(
            "\t".join([
                test["label"],
                test["unit"],
                format_rate(test["row1_rate"]),
                format_rate(test["row2_rate"]),
                format_p_value(test["fisher_exact_p"]),
                odds_text,
            ])
        )


def main():
    parser = argparse.ArgumentParser(
        description="Analyze whether Caucasian terminology persists across papers sharing an author."
    )
    parser.add_argument(
        "--recent-start-year",
        type=int,
        default=DEFAULT_RECENT_START_YEAR,
        help="First year treated as recent for attrition/substitution cohort tests",
    )
    parser.add_argument(
        "--peak-year",
        type=int,
        default=DEFAULT_PEAK_YEAR,
        help="Peak Caucasian year used for reduction decomposition",
    )
    parser.add_argument("--output-json", help="Write full statistics to this JSON file")
    parser.add_argument("--output-csv", help="Write 2x2 test table to this CSV file")
    parser.add_argument("--output-annual-csv", help="Write annual decomposition to this CSV file")
    parser.add_argument("--output-html", help="Write a standalone HTML report to this file")
    parser.add_argument(
        "--source-sqlite",
        default=(
            os.environ.get("CROSSREF_AUTHOR_SOURCE_SQLITE")
            or os.environ.get("CROSSREF_RETRACTION_SOURCE_SQLITE")
        ),
        help="Focused Crossref SQLite stage to use as the author metadata source",
    )
    args = parser.parse_args()

    conn = psycopg2.connect("")
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("SET search_path TO languageingenetics, public")
    if args.source_sqlite and os.path.exists(args.source_sqlite):
        rows = load_author_source_rows_from_sqlite(cursor, args.source_sqlite)
    else:
        cursor.execute(AUTHOR_SOURCE_SQL)
        rows = cursor.fetchall()
    cursor.close()
    conn.close()

    stats = build_author_persistence_statistics(
        rows,
        recent_start_year=args.recent_start_year,
        peak_year=args.peak_year,
    )

    if args.output_json:
        with open(args.output_json, "w") as output:
            json.dump(stats, output, indent=2)
            output.write("\n")

    if args.output_csv:
        write_author_persistence_csv(stats, args.output_csv)

    if args.output_annual_csv:
        write_author_persistence_annual_csv(stats, args.output_annual_csv)

    if args.output_html:
        write_author_persistence_html(stats, args.output_html)

    print_summary(stats)


if __name__ == "__main__":
    main()
