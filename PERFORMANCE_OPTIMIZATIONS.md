# Performance Optimization Recommendations

Based on analysis of `query_explains.log` and `bulkquery_explains.log`, here are the bottlenecks and fixes:

## Critical Issues Found

### 1. 🔴 CRITICAL: Dashboard journal discovery query (24 SECONDS!)
**File:** `generate_dashboard.py:509-526`
**Current execution time:** 24,033 seconds (6.7 hours!)
**Issue:** Parallel sequential scan of entire `raw_text_data` table (hundreds of GB) looking for journals with "genetic" in the name

```
Parallel Seq Scan on public.raw_text_data
Rows Removed by Filter: 55,482,167
Execution Time: 24033266.593 ms
```

**Solution:** Use the materialized view instead of the fallback query. The fallback query scans the entire database!

**Fix:** Remove the fallback query entirely. If `journals_mv` doesn't exist, just skip that data or create the MV first.

```python
# Current code (lines 478-537) - REMOVE THE FALLBACK
try:
    execute_query("""SELECT ... FROM public.journals_mv ...""")
    using_mv = True
except psycopg2.Error:
    # THIS FALLBACK SCANS THE ENTIRE DATABASE - DON'T USE IT
    conn.rollback()
    using_mv = False
    # DELETE THIS ENTIRE SECTION
```

**Impact:** Reduces dashboard generation from ~6-7 hours to ~1-2 minutes

---

### 2. 🟡 MEDIUM: Per-journal article count queries (131 seconds)
**File:** `generate_dashboard.py:160-176`
**Current execution time:** 131 seconds per journal × 17 journals = ~37 minutes
**Issue:** Each journal requires a separate query with JSONB extraction

```sql
SELECT COUNT(*) as total
FROM public.raw_text_data
WHERE (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title' @> %s::jsonb)
```

**Solution:** The `journals_mv` materialized view already has this data! Just use `article_count` from the MV.

**Fix:** Only query `raw_text_data` if the journal is NOT in the MV:

```python
for journal in enabled_journals:
    # Check if we have MV data for this journal
    if using_mv and journal in journals_mv_data:
        total = journals_mv_data[journal]['article_count']
    else:
        # Only query raw_text_data if we don't have MV data
        execute_query("""
            SELECT COUNT(*) as total
            FROM public.raw_text_data
            WHERE (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb->'container-title' @> %s::jsonb)
        """, [json.dumps([journal])])
        total = cursor.fetchone()['total']
```

**Impact:** Reduces dashboard generation by ~30-40 minutes

---

### 3. 🟢 LOW: bulkquery.py article fetching (6.5 seconds)
**File:** `bulkquery.py:198-226`
**Current execution time:** 6.5 seconds
**Issue:** UNION ALL query is already using the GIN index correctly, but it's still slow

**Current performance is acceptable** - this is scanning a huge table and 6.5 seconds for 2000 articles is reasonable.

**Optional optimization:** Add a cursor-based limit to fetch only unprocessed articles:
```python
# Add to the combined query
query = "SELECT * FROM (\n" + "\n    UNION ALL\n".join(subqueries) + "\n) AS combined"
query += " WHERE id NOT IN (SELECT article_id FROM languageingenetics.files)"
if args.limit:
    query += f" LIMIT {args.limit}"
```

But this might make the query slower. Current performance is fine.

---

## Summary of Recommended Actions

### Immediate Actions (Critical - Do Now)
1. **Remove the fallback journal discovery query** in `generate_dashboard.py:498-536`
   - Either require `journals_mv` to exist, or skip journal stats if it doesn't
   - This single change saves **6-7 hours** per dashboard generation

2. **Use `journals_mv` data for article counts** instead of querying `raw_text_data`
   - Modify the journal statistics loop (lines 158-184)
   - Saves **30-40 minutes** per dashboard generation

### Total Expected Speedup
- **Before:** 6-7 hours for dashboard generation
- **After:** 1-2 minutes for dashboard generation
- **Speedup:** ~200-400x faster

### How to Verify the Fix
After making changes, check the execution times:
```bash
grep "Execution Time:" query_explains.log | awk '{print $3}' | sort -n | tail -5
```

All queries should be under 5 seconds.

---

## Additional Optimizations (Optional)

### Disable EXPLAIN logging in production
The `--explain-queries` flag in `cronscript.sh` adds overhead:
- Each query runs twice (once with EXPLAIN, once for real)
- EXPLAIN logs grow unbounded

**Recommendation:** Only enable EXPLAIN when debugging, not in cron jobs.

```bash
# In cronscript.sh, remove --explain-queries flags:
uv run bulkquery.py --limit "$BATCH_SIZE"  # Remove --explain-queries
uv run generate_dashboard.py --output-dir "$DASHBOARD_DIR"  # Remove --explain-queries
```

---

## Database Maintenance

### Check if GIN index exists
The CLAUDE.md mentions creating a GIN index on `raw_text_data`. Verify it exists:

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'raw_text_data'
  AND schemaname = 'public';
```

Should show:
```
idx_raw_text_data_journal | CREATE INDEX ... USING gin (...)
```

If missing, create it (takes hours on large table):
```sql
CREATE INDEX idx_raw_text_data_journal
ON public.raw_text_data
USING GIN ((regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'container-title'));
```

### Ensure journals_mv is refreshed regularly
Add to cron or cronscript.sh:
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY public.journals_mv;
```

This keeps the MV up-to-date so the dashboard doesn't need to query `raw_text_data`.
