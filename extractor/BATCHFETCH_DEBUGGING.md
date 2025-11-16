# Batchfetch.py Debugging Improvements

## Problem
The script was crashing with `ValueError: A string literal cannot contain NUL (0x00) characters` when trying to insert OpenAI batch results into PostgreSQL.

## Solution
Added defensive programming with comprehensive error handling and logging to:
1. Continue processing even when encountering bad data
2. Log detailed information for debugging
3. Sanitize strings to remove NUL characters

## Key Changes

### 1. String Sanitization
Added `sanitize_string()` function that removes NUL characters (0x00) from text fields before database insertion:
```python
european_phrase = sanitize_string(arguments.get('european_phrase_used', ''))
other_phrase = sanitize_string(arguments.get('other_phrase_used', ''))
```

### 2. Error Handling
Wrapped record processing in try-except blocks that:
- Continue processing other records when one fails
- Log detailed error information including:
  - Article ID
  - Error type and message
  - Raw data that caused the error
  - Hex representation of string fields (to see invisible characters)

### 3. Comprehensive Logging
Added structured logging to track:
- Batch processing progress
- Number of records succeeded/failed per batch
- Overall statistics at the end
- NUL character removal warnings
- Full error details to `batchfetch_errors.log`

### 4. Exit Codes
- Exit 0: All successful or partial success (some records processed)
- Exit 1: Complete failure (no records processed)

## Usage

### Normal Operation
```bash
cd extractor/
uv run batchfetch.py --report-costs
```

### Check Error Log
```bash
cat extractor/batchfetch_errors.log
```

### Custom Error Log Location
```bash
uv run batchfetch.py --error-log /path/to/custom/error.log
```

## Log Output Example

Success with sanitization:
```
[2025-11-16 17:44:09] INFO: Found 3 batch(es) to process
[2025-11-16 17:44:09] INFO: Processing batch 47 (OpenAI ID: batch_6915...)
[2025-11-16 17:44:13] WARNING: Removed 2 NUL character(s) from string
[2025-11-16 17:44:14] INFO: Batch 47 complete: 2233 succeeded, 0 failed
[2025-11-16 17:44:21] INFO: Processing complete!
[2025-11-16 17:44:21] INFO: Records succeeded: 6694
[2025-11-16 17:44:21] INFO: Records failed: 0
```

Error example (if sanitization failed):
```
[2025-11-16 12:00:00] ERROR: Failed to process record in batch 47
[2025-11-16 12:00:00] ERROR: Error type: ValueError
[2025-11-16 12:00:00] ERROR: Error message: A string literal cannot contain NUL (0x00) characters
[2025-11-16 12:00:00] ERROR: Article ID: 12345
[2025-11-16 12:00:00] ERROR: Arguments: {
  "caucasian": false,
  "european_phrase_used": "some text with \u0000 NUL"
}
[2025-11-16 12:00:00] ERROR: european_phrase_used (hex): 73 6f 6d 65 20 74 65 78 74 20 77 69 74 68 20 00 20 4e 55 4c
```

## Daily Monitoring

Since this runs daily via cron, you can:
1. Check `cronscript.log` for high-level status
2. Check `batchfetch_errors.log` for detailed error information
3. Look for WARNING/ERROR messages in the logs
4. Monitor the "Records failed" count in daily runs

## Finding Problematic Records

If you need to investigate specific articles with NUL characters:
```sql
-- Find recently processed articles from a specific batch
SELECT article_id, european_phrase_used, other_phrase_used
FROM languageingenetics.files
WHERE batch_id = 47
AND when_processed > NOW() - INTERVAL '1 day';
```

## Next Steps for Investigation

If NUL characters continue to be a problem:
1. Check the error log for the hex representation of affected strings
2. Investigate which articles are producing NUL characters
3. Determine if the issue is in OpenAI's API responses
4. Consider reporting to OpenAI if it's a consistent API issue
