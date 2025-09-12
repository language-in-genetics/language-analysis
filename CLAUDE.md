# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a research project focused on analyzing word frequency in academic articles, specifically examining racial/ethnic terminology usage in genetics journals. The project has two main components:

1. **Data Extraction**: Downloads and processes CrossRef database dumps to extract article metadata from specific genetics journals
2. **Content Analysis**: Uses OpenAI's API to analyze article titles and abstracts for racial/ethnic terminology usage

## Architecture

### Go Components (`cmd/` directory)
- **jsonreader**: Parses compressed CrossRef JSON dumps and extracts articles from target journals, saving metadata as individual JSON files
- **pgjsontool**: Alternative tool that loads extracted JSON data directly into PostgreSQL database

### Python Analysis Pipeline (`extractor/` directory)
- **bulkquery.py**: Creates OpenAI batch jobs to analyze articles for racial terminology
- **batchcheck.py**: Monitors the status of OpenAI batch processing jobs
- **batchfetch.py**: Retrieves completed batch results and stores them in SQLite database

### Data Storage
- **File-based**: Individual JSON files organized by journal name in `articles/` directory
- **Database**: PostgreSQL for structured storage of article metadata and SQLite for analysis results

## Common Development Commands

### Building the Go tools
```bash
make all                    # Build both jsonreader and pgjsontool
make bin/jsonreader        # Build only jsonreader
make bin/pgjsontool        # Build only pgjsontool
make clean                 # Remove built binaries
```

### Testing and Linting
```bash
make test                  # Run Go tests
make lint                  # Run golangci-lint (requires installation)
make dev-deps             # Install development dependencies
```

### Running the Data Extraction Pipeline
```bash
# Extract articles from CrossRef dump
./bin/jsonreader -dir "crossref-dump-directory" -output "articles"

# Alternative: Load into PostgreSQL
./bin/pgjsontool -dir "crossref-dump-directory" -db "postgres://user:pass@localhost/crossref"
```

### Python Analysis Workflow
```bash
cd extractor/

# Set up Python environment (if not already done)
python -m venv .venv
source .venv/bin/activate  # On Linux/Mac
pip install -r ../requirements.txt

# Run the analysis pipeline
./bulkquery.py --limit 1000 --database openai-batch.sqlite
./batchcheck.py --database openai-batch.sqlite
./batchfetch.py --database openai-batch.sqlite

# Export results to CSV
sqlite3 openai-batch.sqlite -header -csv "SELECT * FROM files;" > ancestry-full.csv
```

## Key Configuration

### Target Journals
The `jsonreader` tool is configured to extract articles from specific genetics journals. The journal list is defined in `cmd/jsonreader/main.go` in the `targetJournals` map. Most journals are currently commented out except for "The American Journal of Human Genetics".

### Database Schema
- **PostgreSQL**: Uses `database/schema.sql` for article metadata storage
- **SQLite**: Stores OpenAI analysis results with structured output from the race/ethnicity detection prompts

## Development Notes

- The Go module is named `crossref-parser` and requires Go 1.21+
- Uses PostgreSQL driver (`github.com/lib/pq`) for database operations
- Python components require OpenAI API access for batch processing
- The project processes compressed JSON files (`.json.gz`) from CrossRef academic database dumps