#!/bin/bash
# Hourly batch check script for cron
# Add to cron with: 0 * * * * /home/languageingenetics/Word-Frequency-Analysis-/batchcheck_cron.sh

set -e

# Change to project directory
cd /home/languageingenetics/Word-Frequency-Analysis-/extractor

# Ensure database is set
export PGDATABASE=crossref

# Run batch check
uv run batchcheck.py --quiet
