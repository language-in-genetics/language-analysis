# Random Sampling Tools

Two tools are available for reproducibly selecting random papers from your focused journals.

## Quick Start (Recommended)

For most use cases, use the **quick** version which samples from already-processed articles:

```bash
cd extractor/

# Select 500 random processed papers
./quick_random_sample.py --output sample_500.csv

# Same command will always give the same 500 papers (seed=42 by default)
./quick_random_sample.py --output sample_500_again.csv  # Identical to above

# Different seed = different random sample
./quick_random_sample.py --seed 99 --output different_sample.csv
```

## Tools Overview

### `quick_random_sample.py` ⚡ (Recommended)

**Purpose**: Fast random sampling from processed articles

**Speed**: ~2-5 seconds for 500 papers

**Advantages**:
- Very fast (queries only the processed articles)
- Includes full analysis results
- Perfect for analyzing terminology usage patterns

**Limitations**:
- Only samples from articles that have been processed by OpenAI
- Cannot sample from unprocessed articles

**When to use**: Most research scenarios where you want to analyze papers with terminology classifications

### `random_sample.py` 🐌 (Slower, more comprehensive)

**Purpose**: Random sampling from ALL articles (including unprocessed)

**Speed**: ~2-5 minutes for 500 papers (depending on database size)

**Advantages**:
- Can sample from both processed and unprocessed articles
- Allows `--processed-only` flag to behave like quick version

**Limitations**:
- Much slower due to large dataset joins
- Performance depends on database size

**When to use**: When you need to sample from unprocessed articles, or want a random sample of all articles regardless of processing status

## Usage Examples

### Quick Random Sample (fast)

```bash
# Basic usage: 500 random processed papers
./quick_random_sample.py --output sample.csv

# Select fewer papers
./quick_random_sample.py --sample-size 100 --output sample_100.csv

# Filter by year range
./quick_random_sample.py --min-year 2015 --max-year 2020 --output recent.csv

# Filter by journal
./quick_random_sample.py --journal "The American Journal of Human Genetics" --output ajhg.csv

# Different random seed for different sample
./quick_random_sample.py --seed 12345 --output sample_seed12345.csv

# Combine filters
./quick_random_sample.py --min-year 2010 --journal "Nature Genetics" --sample-size 200 --output ng_2010plus.csv
```

### Full Random Sample (slower, includes unprocessed)

```bash
# Sample from ALL articles (processed and unprocessed)
./random_sample.py --output all_articles_sample.csv

# Sample only from processed articles (similar to quick version, but slower)
./random_sample.py --processed-only --output processed_sample.csv

# Sample from unprocessed articles only
# (Note: requires modifying the code to add --unprocessed-only flag)
```

## Reproducibility

Both tools use a random seed (default: 42) to ensure reproducible results.

**Same seed = same papers**:
```bash
# These three commands will select the EXACT same 500 papers:
./quick_random_sample.py --seed 42 --output run1.csv
./quick_random_sample.py --seed 42 --output run2.csv
./quick_random_sample.py --output run3.csv  # seed defaults to 42
```

**Different seed = different papers**:
```bash
./quick_random_sample.py --seed 42 --output set1.csv    # One random set
./quick_random_sample.py --seed 99 --output set2.csv    # Different random set
./quick_random_sample.py --seed 12345 --output set3.csv # Another different set
```

## Output Format

Both tools output CSV files with these columns:

| Column | Description |
|--------|-------------|
| `article_id` | Unique article identifier |
| `journal_name` | Name of the journal |
| `doi` | Digital Object Identifier |
| `title` | Article title (as JSON string from CrossRef) |
| `pub_year` | Publication year |
| `abstract` | Article abstract text |
| `article_type` | CrossRef article type (e.g., "journal-article") |
| `is_processed` | Whether OpenAI analysis is complete |
| `has_abstract` | Whether article has an abstract |
| `when_processed` | Timestamp when analysis was completed |
| `caucasian` | Whether "Caucasian" terminology was found |
| `white` | Whether "White" terminology was found |
| `european` | Whether "European" terminology was found |
| `european_phrase_used` | Specific European-related phrase found |
| `other` | Whether other racial/ethnic terminology was found |
| `other_phrase_used` | Specific other phrase found |
| `prompt_tokens` | OpenAI prompt tokens used |
| `completion_tokens` | OpenAI completion tokens used |

## Summary Statistics

Both tools print summary statistics to stderr:

```
Finding processed articles (seed=42)...
Found 54387 processed articles
Randomly selected 500 articles
Fetching full details...
Retrieved 500 papers
Wrote results to sample_500.csv

Summary:
  Total papers: 500
  Journals represented: 17
    Heredity: 89
    The American Journal of Human Genetics: 82
    European Journal of Human Genetics: 74
    ...
  Year range: 1947-2025
  Terminology usage:
    Caucasian: 2 (0.4%)
    White: 1 (0.2%)
    European: 5 (1.0%)
    Other: 20 (4.0%)
```

## Common Workflows

### Research Paper Manual Review

Select a random sample for manual review:

```bash
# Get 100 random papers from the last 10 years
./quick_random_sample.py --sample-size 100 --min-year 2015 --output manual_review.csv
```

### Validation Sample

Create a reproducible validation set:

```bash
# Always use the same seed for your validation set
./quick_random_sample.py --seed 2024 --sample-size 200 --output validation_set.csv
```

### Journal-Specific Analysis

Sample from each journal separately:

```bash
./quick_random_sample.py --journal "The American Journal of Human Genetics" --output ajhg_sample.csv
./quick_random_sample.py --journal "European Journal of Human Genetics" --output ejhg_sample.csv
./quick_random_sample.py --journal "Nature Genetics" --output ng_sample.csv
```

### Time Period Comparison

Compare different time periods:

```bash
./quick_random_sample.py --min-year 1990 --max-year 1999 --seed 1 --output sample_1990s.csv
./quick_random_sample.py --min-year 2000 --max-year 2009 --seed 1 --output sample_2000s.csv
./quick_random_sample.py --min-year 2010 --max-year 2019 --seed 1 --output sample_2010s.csv
./quick_random_sample.py --min-year 2020 --max-year 2029 --seed 1 --output sample_2020s.csv
```

## Performance Notes

### Quick Random Sample Performance

- **54,387 processed articles**: ~2-5 seconds for 500 samples
- Performance scales well with database size
- Uses indexed queries for fast lookups

### Full Random Sample Performance

- First query to get candidate IDs can be slow (2-10 minutes depending on database size)
- Subsequent queries with filters may be faster
- Performance depends on:
  - Total database size
  - Number of enabled journals
  - GIN index on `raw_text_data.filesrc`

### Optimization Tips

1. **Use quick_random_sample.py when possible** - it's much faster
2. **Filter by journal** to reduce the candidate set
3. **Filter by year range** to further narrow results
4. **Ensure GIN index exists** on raw_text_data (see CLAUDE.md)

## Troubleshooting

### "No matching articles found"

- Check that journals are enabled: `psql -c "SELECT name, enabled FROM languageingenetics.journals;"`
- Check that articles have been processed: `psql -c "SELECT COUNT(*) FROM languageingenetics.files WHERE processed = true;"`
- Verify year range is valid
- Verify journal name matches exactly (case-sensitive)

### Slow performance with random_sample.py

- Use quick_random_sample.py instead if you only need processed articles
- Ensure the GIN index exists on raw_text_data
- Consider adding filters (--journal, --min-year, --max-year) to reduce the search space

### Different results each time

- Make sure you're using the same `--seed` value
- Default seed is 42, so omitting `--seed` should give consistent results
- Check that the underlying data hasn't changed (new articles processed)

## Technical Details

Both tools use Python's `random.sample()` function with a fixed seed for reproducibility. The sampling process:

1. Query database for candidate article IDs matching filters
2. Load all matching IDs into memory
3. Set random seed
4. Use `random.sample()` to select N random IDs
5. Fetch full details from the view for selected IDs only
6. Output to CSV

This two-stage approach (IDs first, then details) is much faster than trying to randomly sort the entire joined view.
