# Focused Journals View - Usage Examples

The `languageingenetics.focused_journals_view` provides an easy way to query articles from your enabled journals.

## Basic Queries

### Count articles per journal
```sql
SELECT
    journal_name,
    COUNT(*) AS total_articles,
    COUNT(CASE WHEN is_processed THEN 1 END) AS processed,
    COUNT(CASE WHEN NOT is_processed OR is_processed IS NULL THEN 1 END) AS pending
FROM languageingenetics.focused_journals_view
GROUP BY journal_name
ORDER BY total_articles DESC;
```

### Get all processed articles with terminology usage
```sql
SELECT
    journal_name,
    pub_year,
    doi,
    title,
    caucasian,
    white,
    european,
    european_phrase_used,
    other,
    other_phrase_used
FROM languageingenetics.focused_journals_view
WHERE is_processed = true
    AND (caucasian OR white OR european OR other)
ORDER BY pub_year DESC, journal_name;
```

### Articles using "Caucasian" terminology
```sql
SELECT
    journal_name,
    pub_year,
    doi,
    title
FROM languageingenetics.focused_journals_view
WHERE caucasian = true
ORDER BY pub_year DESC;
```

### Terminology trends by year
```sql
SELECT
    pub_year,
    COUNT(*) AS total_processed,
    SUM(CASE WHEN caucasian THEN 1 ELSE 0 END) AS caucasian_count,
    SUM(CASE WHEN white THEN 1 ELSE 0 END) AS white_count,
    SUM(CASE WHEN european THEN 1 ELSE 0 END) AS european_count,
    SUM(CASE WHEN other THEN 1 ELSE 0 END) AS other_count,
    ROUND(100.0 * SUM(CASE WHEN caucasian THEN 1 ELSE 0 END) / COUNT(*), 2) AS caucasian_pct
FROM languageingenetics.focused_journals_view
WHERE is_processed = true
    AND pub_year IS NOT NULL
GROUP BY pub_year
ORDER BY pub_year;
```

### Most recent unprocessed articles
```sql
SELECT
    journal_name,
    pub_year,
    doi,
    title,
    has_abstract
FROM languageingenetics.focused_journals_view
WHERE is_processed IS NULL OR is_processed = false
ORDER BY pub_year DESC
LIMIT 100;
```

### Export to CSV
```bash
# All processed articles with analysis results
psql -c "\copy (SELECT * FROM languageingenetics.focused_journals_view WHERE is_processed = true) TO 'focused_journals_analysis.csv' WITH CSV HEADER"

# Summary by journal and year
psql -c "\copy (SELECT journal_name, pub_year, COUNT(*) AS articles, SUM(CASE WHEN caucasian THEN 1 ELSE 0 END) AS caucasian, SUM(CASE WHEN white THEN 1 ELSE 0 END) AS white, SUM(CASE WHEN european THEN 1 ELSE 0 END) AS european FROM languageingenetics.focused_journals_view WHERE is_processed = true GROUP BY journal_name, pub_year ORDER BY journal_name, pub_year) TO 'terminology_by_journal_year.csv' WITH CSV HEADER"
```

## Advanced Queries

### Find articles with specific phrases
```sql
SELECT
    journal_name,
    pub_year,
    doi,
    title,
    european_phrase_used,
    other_phrase_used
FROM languageingenetics.focused_journals_view
WHERE (european_phrase_used IS NOT NULL AND european_phrase_used != '')
   OR (other_phrase_used IS NOT NULL AND other_phrase_used != '')
ORDER BY pub_year DESC;
```

### Token usage statistics
```sql
SELECT
    journal_name,
    COUNT(*) AS processed_articles,
    SUM(prompt_tokens) AS total_prompt_tokens,
    SUM(completion_tokens) AS total_completion_tokens,
    AVG(prompt_tokens) AS avg_prompt_tokens,
    AVG(completion_tokens) AS avg_completion_tokens
FROM languageingenetics.focused_journals_view
WHERE is_processed = true
GROUP BY journal_name
ORDER BY processed_articles DESC;
```

### Processing progress
```sql
SELECT
    journal_name,
    COUNT(*) AS total_articles,
    COUNT(CASE WHEN is_processed THEN 1 END) AS processed,
    ROUND(100.0 * COUNT(CASE WHEN is_processed THEN 1 END) / COUNT(*), 2) AS pct_complete
FROM languageingenetics.focused_journals_view
GROUP BY journal_name
ORDER BY pct_complete DESC;
```

## View Columns Reference

| Column | Description |
|--------|-------------|
| `article_id` | Unique article identifier from raw_text_data |
| `journal_name` | Name of the journal |
| `journal_table_id` | ID from journals table |
| `journal_enabled` | Whether journal is enabled (always true in this view) |
| `doi` | Digital Object Identifier |
| `title` | Article title |
| `pub_year` | Publication year |
| `abstract` | Article abstract text |
| `article_type` | CrossRef article type |
| `analysis_id` | ID from files table (NULL if not processed) |
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
| `batch_id` | OpenAI batch job identifier |

## Notes

- The view only shows articles from journals where `enabled = true` in the `languageingenetics.journals` table
- If you disable a journal, it will automatically disappear from this view
- The view uses LEFT JOIN for analysis results, so unprocessed articles appear with NULL analysis fields
- For performance, consider adding WHERE clauses to filter by `pub_year`, `journal_name`, or `is_processed`
