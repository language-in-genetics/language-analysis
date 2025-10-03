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
