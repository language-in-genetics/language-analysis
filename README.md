# Word Frequency Analysis - Language in Genetics

A research project analyzing racial/ethnic terminology usage in genetics journals. The system processes CrossRef article metadata and uses OpenAI's API to identify terminology patterns in academic publications.

## Architecture

- **PostgreSQL Database**: Stores CrossRef article metadata and analysis results
- **Go Tools** (`cmd/pgjsontool`): Bulk import tool for CrossRef dump files
- **Python Pipeline** (`extractor/`): OpenAI batch processing for content analysis
- **Automated Dashboard**: Generates static HTML dashboards showing analysis results

## Quick Start

### Prerequisites

- PostgreSQL with the CrossRef database imported
- Python 3.x with virtual environment
- OpenAI API key
- Go 1.21+ (only if importing new CrossRef data)

### Setup

```bash
# Set database environment variable
export PGDATABASE=crossref

# Set up Python environment
cd extractor/
python -m venv .venv
source .venv/bin/activate
pip install -r ../requirements.txt
```

### Running Analysis

```bash
cd extractor/


## Computational Analysis Methodology

The `extractor/bulkquery.py` script implements an automated approach to analyze genetics articles for specific racial terminology. It processes metadata.json files containing article titles and abstracts, then submits them to OpenAI's API for analysis.

The core of the analysis uses a carefully constructed prompt:
```
"Does this article use any terms like \"Caucasian\" or \"white\" or \"European ancestry\" in a way that refers to race, ancestry, ethnicity or population?\n\n"
"TITLE: {title}\n"
"ABSTRACT: {abstract}\n"
```

This prompt is deliberately framed in a neutral manner to avoid biasing the language model's analysis. It specifically asks about terms related to European ancestry without suggesting preference for any particular terminology.

The analysis is structured through a function-calling API that forces OpenAI to return standardized responses across all articles. The analysis function includes parameters for detecting:
- "caucasian" terminology
- "white" racial descriptors
- "European ancestry" phrasing
- Other phrases describing European populations

When phrases are detected, the system also captures the exact terminology used, enabling detailed analysis of language variations across the literature.

The batch processing system allows efficient processing of thousands of articles with proper error handling and progress tracking, making large-scale analysis feasible within reasonable time and cost constraints.


# Process articles from enabled journals
./bulkquery.py --limit 1000

# Check batch status
./batchcheck.py

# Fetch completed results
./batchfetch.py

# Generate dashboard
./generate_dashboard.py --output-dir ../dashboard
```

### Automation

The `cronscript.sh` script automates the entire workflow:

```bash
# Run manually
./cronscript.sh

# Schedule with cron (every 6 hours)
crontab -e
# Add: 0 */6 * * * /home/languageingenetics/Word-Frequency-Analysis-/cronscript.sh
```

## Database Schema

**public.raw_text_data**: CrossRef article metadata (JSON in `filesrc` column)

**languageingenetics schema**:
- `journals`: Manages which journals to process
- `files`: Analysis results per article
- `batches`: OpenAI batch job tracking

## Managing Journals

```sql
-- List journals and their status
SELECT name, enabled FROM languageingenetics.journals ORDER BY name;

-- Disable a journal
UPDATE languageingenetics.journals SET enabled = false WHERE name = 'Heredity';

-- Add a new journal
INSERT INTO languageingenetics.journals (name) VALUES ('New Journal Name');
```

## Development

```bash
# Build Go tools (only needed for importing new data)
make all

# Run tests
make test

# Run linter
make lint
```

## Documentation

See [CLAUDE.md](CLAUDE.md) for detailed project documentation, including:
- Complete architecture details
- Database setup and permissions
- Initial import procedures
- Python workflow details
- Configuration options 
