# Crossref SQLite Cache Prefilter Pipeline

Status: implemented for smoke testing.

This pipeline is the safe path for loading annual Crossref snapshots after the
2025 legacy backfill. It avoids using PostgreSQL as a row-by-row lookup engine
while reading the annual dump, and it avoids rewriting the legacy
`public.crossref_work_versions` table just to fill new helper columns.

## Why This Exists

The 2025 baseline was loaded before `text_fingerprint` existed. The live
database therefore has roughly 167 million current version rows where
`text_fingerprint` is usually `NULL`.

An in-place backfill is the wrong fix. Updating the large
`public.crossref_work_versions` heap would create a new tuple version for every
row, update indexes, generate a large amount of WAL, and leave dead tuples that
vacuum must later clean. This is exactly the workload that performs badly on the
current storage.

A separate PostgreSQL table would be better than updating the large heap, but it
would still write roughly 167 million rows plus an index. If the consumer is a
fast lookup cache, PostgreSQL is still an unnecessary middle layer.

The chosen design is:

1. Stream current DOI rows out of PostgreSQL once.
2. Build a SQLite lookup cache on disk.
3. Compute missing legacy `text_fingerprint` values while building that cache.
4. Copy the SQLite cache into RAM for classification.
5. Stream the Crossref annual dump and write only `new` and `changed` records to
   compact JSONL gzip files.
6. Import only those compact files into PostgreSQL.

## What `text_fingerprint` Means

`text_fingerprint` is a SHA-256 digest over normalized title and abstract:

```text
sha256(normalize(title) || "\x1f" || normalize(abstract))
```

Normalization lowercases text, unescapes common HTML entities, strips tags,
normalizes Unicode dash variants, collapses whitespace, and trims.

The field is not a raw Crossref JSON hash. It deliberately ignores Crossref
bookkeeping churn such as `indexed`, `deposited`, citation counts, references,
links, and other fields that do not change the title/abstract analysed by this
project.

Policy:

- Keep `public.crossref_work_versions.text_fingerprint` nullable.
- Populate it for new or genuinely changed semantic versions going forward.
- Do not backfill it into the 2025 legacy rows in PostgreSQL.
- When a cache needs a fingerprint and the database value is `NULL`, compute it
  from the row's `title` and `abstract` while building the cache.

## Cache Schema

The SQLite cache stores one row per current DOI:

```sql
CREATE TABLE doi_cache (
    doi_hash BLOB PRIMARY KEY,
    text_fingerprint BLOB NOT NULL,
    work_id INTEGER NOT NULL,
    work_version_id INTEGER NOT NULL
) WITHOUT ROWID;
```

The DOI hash is `sha256(lower(trim(normalized_doi)))`. Storing the hash keeps the
lookup key fixed-width and compact. `text_fingerprint` is always present in the
cache, even when it had to be computed from legacy title/abstract fields.

## Tools

Build the binaries:

```bash
make bin/crossrefcachebuild bin/crossrefclassify bin/crossrefimport
```

Build a SQLite cache:

```bash
./bin/crossrefcachebuild \
  -format sqlite \
  -compute-missing-fingerprints \
  -out /dbtemp/March\ 2026\ Public\ Data\ File\ from\ Crossref/_prefiltered_full_import/current-doi-cache.sqlite \
  -manifest /dbtemp/March\ 2026\ Public\ Data\ File\ from\ Crossref/_prefiltered_full_import/current-doi-cache.sqlite.manifest.json \
  -report-every 1000000
```

Classify a dump against the cache:

```bash
./bin/crossrefclassify \
  -cache /dbtemp/March\ 2026\ Public\ Data\ File\ from\ Crossref/_prefiltered_full_import/current-doi-cache.sqlite \
  -cache-format sqlite \
  -sqlite-copy-to-memory \
  -dir /dbtemp/March\ 2026\ Public\ Data\ File\ from\ Crossref \
  -out-dir /dbtemp/March\ 2026\ Public\ Data\ File\ from\ Crossref/_prefiltered_full_import/classified \
  -workers 8
```

The classifier writes:

- `new.jsonl.gz`: DOI not present in the current cache
- `changed.jsonl.gz`: DOI present but title/abstract fingerprint differs
- `unknown-fingerprint.jsonl.gz`: cache row has no usable fingerprint
- `no-doi.jsonl.gz`: dump row has no DOI
- `summary.json`: classification counts

With `-compute-missing-fingerprints` used during cache build,
`unknown-fingerprint` should be zero or very small. A large value means the cache
was built without usable text fingerprints and should not be used for full
classification.

## Wrapper Script

The reusable wrapper is:

```bash
database/run_crossref_prefilter_pipeline.sh
```

By default it builds the SQLite cache, classifies the dump, prepares an
`importable/` directory containing symlinks to `new.jsonl.gz` and
`changed.jsonl.gz`, and stops before import.

Run a classification-only pass:

```bash
database/run_crossref_prefilter_pipeline.sh
```

Run the import after classification:

```bash
RUN_IMPORT=1 \
RUN_LABEL=crossref-2026-annual-prefiltered \
database/run_crossref_prefilter_pipeline.sh
```

Useful environment variables:

- `DUMP_DIR`: Crossref dump directory
- `WORK_ROOT`: pipeline output directory
- `CACHE_PATH`: SQLite cache path
- `BUILD_CACHE=0`: reuse an existing cache
- `WORKERS`: gzip reader workers for classification
- `BATCH_SIZE`: importer batch size
- `INCLUDE_UNKNOWN=1`: include `unknown-fingerprint` records in the import

Do not set `INCLUDE_UNKNOWN=1` for routine full imports. It is for diagnostic
runs only.

## Crash-Reproduction Debug Run

If the cache build appears to hard-lock or reboot `raksasa`, use the debug
launcher instead of the normal wrapper:

```bash
database/run_crossref_prefilter_debug.sh
```

It still defaults to `RUN_IMPORT=0`, so it builds the DOI cache and classifies
without importing. The launcher creates a fresh
`/home/languageingenetics/crossref-prefilter-debug-<timestamp>/` work root and
writes several independent evidence streams under `debug/`:

- `summary.tsv`: frequent process, memory, pressure, cache-size, and progress
  samples
- `snapshots/*.txt`: expanded host snapshots with memory, PSI, `vmstat`,
  `iostat`, process tables, disk space, and the pipeline log tail
- `proc/<epoch>/`: `/proc` snapshots for `crossrefcachebuild`,
  `crossrefclassify`, and the wrapper process
- `cachebuild-stats.jsonl`: in-process Go heap plus `/proc/self/status`,
  `/proc/self/smaps_rollup`, `/proc/self/io`, row counts, and cache-file sizes

Useful knobs:

- `MONITOR_INTERVAL=2`: sample the host every two seconds
- `DEBUG_STATS_INTERVAL=100000`: write cache-builder JSONL stats every 100,000
  rows
- `DEBUG_STATS_SYNC=1`: fsync the JSONL stats after each sample

## Operational Checks

Before importing:

1. Inspect `classified/summary.json`.
2. Confirm `unknown` is zero or explainably tiny.
3. Confirm `changed` is plausible; it should be a small fraction of the full
   dump.
4. Confirm `new` is plausible for the snapshot year.
5. Confirm `/dbtemp`, `/crossref`, and PostgreSQL storage have enough free space.

If `changed` is implausibly high, stop. That means the fingerprint comparison is
wrong or the cache was built from the wrong current corpus.

## Current Measurements

The 10 million row SQLite experiment on `raksasa` produced:

- binary cache size: 763 MB
- SQLite cache size: 846 MB
- SQLite disk load from binary: 21.9 seconds
- SQLite disk-to-RAM copy: 0.72 seconds
- 100,000 random SQLite-in-RAM lookups: 0.314 seconds

Scaling from that sample, a full current DOI cache should fit comfortably in
64 GB RAM. The expensive step is not SQLite lookup; it is the one-time read of
`title` and `abstract` from PostgreSQL when legacy fingerprints must be computed.

Do not infer a full-run ETA from the first few hundred thousand rows. The live
database shows strong locality effects: warm early ranges can stream quickly,
while offset-heavy or cold ranges can be much slower. The first full pipeline run
should therefore be monitored from its own progress logs, not from a small SQL
probe.
