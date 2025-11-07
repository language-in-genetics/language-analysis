#!/bin/bash
#
# Example: Generate a reproducible random sample of 500 papers
#
# This script demonstrates how to create a random sample that will
# always be the same (same 500 papers) when run multiple times.
#

cd "$(dirname "$0")"

# Default: 500 papers with seed 42 (default)
./quick_random_sample.py --output sample_500.csv

echo ""
echo "Sample saved to: sample_500.csv"
echo ""
echo "To get a different random sample, use a different seed:"
echo "  ./quick_random_sample.py --seed 99 --output different_sample.csv"
echo ""
echo "To filter by year:"
echo "  ./quick_random_sample.py --min-year 2015 --max-year 2020 --output recent.csv"
echo ""
echo "See SAMPLING_README.md for more examples"
