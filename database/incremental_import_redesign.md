# Incremental Crossref Import Redesign

Status: proposed

This document describes the import redesign needed to support yearly Crossref additions without destroying existing data or breaking downstream analysis.

## What The March 2026 Dump Actually Looks Like

The proposed importer should be built against the real March 2026 Academic Torrents dump shape, not the old assumptions in `cmd/pgjsontool`.

Observed on `raksasa` from the downloaded snapshot:

- the data files are numbered `*.jsonl.gz` files such as `0.jsonl.gz`, `1.jsonl.gz`, `10.jsonl.gz`
- each compressed file contains newline-delimited JSON objects
- each line is one Crossref work record
- the top level is not a JSON object with an `items` array
- the dump directory also contains at least one non-data helper file, `a.py`

Example record traits from sampled files:

- `DOI` present
- `title` usually present and usually a list
- `container-title` usually present and usually a list
- `published` usually present as an object
- `abstract` often absent

Quick sample across 20,000 records from the March 2026 dump:

- `bad_json = 0`
- `missing_doi = 0`
- `missing_title = 104`
- `missing_container_title = 658`
- `missing_published_dict = 75`
- `missing_abstract = 12,152`

Implications:

- a DOI-keyed incremental importer is viable on the actual dump format
- title, journal, abstract, and publication year must all be treated as nullable extracted fields
- the importer must explicitly whitelist `*.jsonl.gz`
- the current `pgjsontool` parser is incompatible with the current snapshot format because it expects a top-level `items` array

## Why This Exists

The current import path was built for a one-time bulk load:

- `database/import.sh` streams raw JSON lines into `public.raw_text_data`
- `public.raw_text_data.id` is a serial row id with no stable meaning outside that table
- the analysis pipeline stores `languageingenetics.files.article_id = raw_text_data.id`

That worked for the March 2025 initial load, but it does not scale to yearly Crossref refreshes. Each annual Crossref dump is a full corpus snapshot, not just the new year. Appending the March 2026 dump to `raw_text_data` would create duplicates for almost every existing DOI, and a full table reload would change row ids and orphan downstream references.

The redesign therefore needs one stable local identifier per Crossref work, plus version history for the raw metadata.

## Goals

- Support yearly imports of full Crossref snapshots without destructive reloads.
- Preserve one stable local identifier for each Crossref work across imports.
- Preserve raw metadata history when Crossref changes a record.
- Keep enough provenance to answer "which dump did this row come from?"
- Let the analysis pipeline work from the current version of each work.
- Detect when a changed title or abstract should trigger re-analysis.
- Keep the operational workflow simple on `raksasa`.

## Non-Goals

- Rewriting the analysis prompts or OpenAI batch workflow in this phase.
- Supporting every possible Crossref delta source on day one.
- Preserving `public.raw_text_data` as the long-term primary table.

## Current Problems

1. `raw_text_data.id` is not stable across reloads.
2. Annual Crossref dumps contain the whole corpus again, so append-only loading duplicates almost everything.
3. `languageingenetics.files.article_id` assumes one row id per article forever.
4. The dashboard and batch scripts query `public.raw_text_data` directly, so the legacy table shape leaks into the whole pipeline.
5. There is no import manifest or run log describing which snapshot produced which rows.

## Proposed Data Model

The redesign separates three concerns:

- one stable local work row
- one version row per changed metadata payload
- one import run row per snapshot ingest

### 1. Import Runs

Create a run table to record each ingest attempt. These Crossref corpus tables are generic infrastructure, so they should live in `public` with explicit `crossref_` names rather than in `languageingenetics`.

```sql
CREATE TABLE public.crossref_import_runs (
    id BIGSERIAL PRIMARY KEY,
    run_label TEXT NOT NULL UNIQUE,
    source_type TEXT NOT NULL,          -- annual_dump, monthly_snapshot, rest_delta
    source_path TEXT NOT NULL,
    snapshot_date DATE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    imported_by TEXT NOT NULL,
    notes TEXT
);
```

Examples of `run_label`:

- `crossref-2025-annual`
- `crossref-2026-annual`
- `crossref-2026-04-rest-delta`

### 2. Stable Works Table

Create one stable row per imported work.

```sql
CREATE TABLE public.crossref_works (
    id BIGSERIAL PRIMARY KEY,
    normalized_doi TEXT,
    original_doi TEXT,
    first_import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    latest_import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX crossref_works_normalized_doi_idx
    ON public.crossref_works(normalized_doi)
    WHERE normalized_doi IS NOT NULL;
```

Rules:

- `id` is the primary key used by downstream tables.
- `normalized_doi` is the preferred stable identifier when present.
- store DOI in a normalized form, for example lowercase and trimmed
- do not use Crossref row position or file number as a durable key
- do not make DOI the primary key because missing-DOI edge cases should not break the schema
- records without a DOI should be rejected or quarantined explicitly until a durable fallback policy exists

### 3. Work Versions Table

Store one row per distinct raw metadata payload seen for a work.

```sql
CREATE TABLE public.crossref_work_versions (
    id BIGSERIAL PRIMARY KEY,
    work_id BIGINT NOT NULL REFERENCES public.crossref_works(id) ON DELETE CASCADE,
    import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    raw_json_text TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    title TEXT,
    abstract TEXT,
    journal_name TEXT,
    pub_year INT,
    record_type TEXT,
    is_current BOOLEAN NOT NULL DEFAULT false,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (work_id, payload_sha256)
);

CREATE UNIQUE INDEX crossref_work_versions_current_idx
    ON public.crossref_work_versions(work_id)
    WHERE is_current;
```

Rules:

- Every import is compared against the current version for the same work.
- If the payload hash is unchanged, do not insert another version row.
- If the payload hash changed, insert a new version row and flip `is_current`.
- Extract a few fields into plain columns so the dashboard and batch queries do not keep reparsing the whole JSON blob.

### 4. Legacy Row Mapping During Migration

During migration, keep a mapping from the old `public.raw_text_data.id` to the new work/version ids.

```sql
CREATE TABLE public.crossref_legacy_raw_text_map (
    raw_text_data_id BIGINT PRIMARY KEY,
    work_id BIGINT NOT NULL REFERENCES public.crossref_works(id),
    work_version_id BIGINT NOT NULL REFERENCES public.crossref_work_versions(id)
);
```

This is the bridge that lets existing `files.article_id` data migrate safely.

### 5. Rejections / Quarantine

Rows that cannot be assigned a durable identity should not disappear silently.

```sql
CREATE TABLE public.crossref_import_rejections (
    id BIGSERIAL PRIMARY KEY,
    import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id) ON DELETE CASCADE,
    source_ref TEXT,
    reason TEXT NOT NULL,
    raw_json_text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

At minimum this should capture:

- missing DOI
- malformed JSON
- impossible publication date extraction, if we decide that should be rejected

## Changes to Analysis Tables

`languageingenetics.files` should stop treating `raw_text_data.id` as the long-term foreign key.

Target shape:

```sql
ALTER TABLE languageingenetics.files
    ADD COLUMN work_id BIGINT,
    ADD COLUMN work_version_id BIGINT;
```

Then backfill and eventually enforce:

- `work_id REFERENCES public.crossref_works(id)`
- `work_version_id REFERENCES public.crossref_work_versions(id)`

Recommended semantics:

- `work_id` identifies the imported work across all imports.
- `work_version_id` identifies the exact metadata payload that was analyzed.

That allows the pipeline to answer:

- "Have we ever analyzed this DOI?"
- "Was the current version analyzed, or only an older version?"

## Recommended Query Surface

Downstream code should query a current-work view instead of the legacy raw table.

```sql
CREATE VIEW public.crossref_current_works AS
SELECT
    w.id AS work_id,
    v.id AS work_version_id,
    w.normalized_doi,
    w.original_doi,
    v.raw_json_text,
    v.title,
    v.abstract,
    v.journal_name,
    v.pub_year,
    v.record_type
FROM public.crossref_works w
JOIN public.crossref_work_versions v
  ON v.work_id = w.id
WHERE v.is_current;
```

The dashboard, sampling scripts, and batch submission code should move to this view.

## Import Algorithm

### Step 1. Register the run

Insert a row in `public.crossref_import_runs` before touching data.

### Step 2. Stream the snapshot into a staging table

Do not upsert directly from shell `COPY` into final tables. Instead:

- stream each `.jsonl.gz` file
- parse the DOI once
- compute a payload hash once
- load into a staging table for the current run

Suggested staging shape:

```sql
CREATE TEMP TABLE import_stage (
    doi TEXT,
    raw_json_text TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    title TEXT,
    abstract TEXT,
    journal_name TEXT,
    pub_year INT,
    record_type TEXT
);
```

This keeps the import resumable and lets SQL handle the final merges in batches.

### Step 3. Upsert stable works

For each distinct DOI in staging:

- insert a new row into `public.crossref_works` if missing
- otherwise update `latest_import_run_id`

Rows without DOI should be written to `public.crossref_import_rejections` and skipped.

### Step 4. Insert only changed versions

For each staged DOI:

- compare `payload_sha256` against the current version for that work
- if unchanged, skip version insert
- if changed, insert a new version row and mark it current

### Step 5. Mark snapshot visibility

Each work should record whether it appeared in the newest full snapshot.

Minimum useful state:

- `latest_import_run_id` in `crossref_works`
- `is_current` in `crossref_work_versions`

Optional later addition:

- a `work_run_presence(work_id, import_run_id)` table if we need exact "present in snapshot X" reporting

### Step 6. Queue re-analysis only when needed

If the current version changed in a way that affects title or abstract, the work should be eligible for a fresh OpenAI batch submission.

A simple rule is enough for the first pass:

- if `payload_sha256` changed and the current version has not been analyzed, it is pending

## Operational Workflow on `raksasa`

The operational convention should be:

1. Download Crossref snapshots onto `raksasa`
2. Keep the canonical dump location under `/crossref/`
3. Run the import as the `languageingenetics` user

Recommended layout:

```text
/crossref/
  crossref-2025-annual/
  crossref-2026-annual/
  incoming/
```

Recommended commands:

- download as `languageingenetics` when possible
- run the importer with `sudo -u languageingenetics ...`

Because the current 2026 torrent was started as `gregb`, the handoff step after completion should be:

```bash
sudo mv "/home/gregb/tmp/crossref-2026/March 2026 Public Data File from Crossref" /crossref/
sudo chown -R languageingenetics:languageingenetics "/crossref/March 2026 Public Data File from Crossref"
```

That gets the data into the correct long-term location even though the active transfer started under the wrong user.

## Migration Plan

### Phase 1. Add the new tables

Add:

- `public.crossref_import_runs`
- `public.crossref_works`
- `public.crossref_work_versions`
- `public.crossref_legacy_raw_text_map`
- `public.crossref_import_rejections`
- `public.crossref_current_works` view

Do this without removing `public.raw_text_data`.

### Phase 2. Backfill the March 2025 baseline

Treat the existing `public.raw_text_data` corpus as one historical import run:

- create `crossref_import_runs` row `crossref-2025-annual`
- populate `crossref_works`
- populate `crossref_work_versions`
- populate `crossref_legacy_raw_text_map`

### Phase 3. Backfill analysis references

Add `work_id` and `work_version_id` to `languageingenetics.files`, then backfill them through `public.crossref_legacy_raw_text_map`.

Keep `article_id` temporarily so existing scripts still run during the migration.

### Phase 4. Move readers to the new view

Update:

- `extractor/bulkquery.py`
- `extractor/random_sample.py`
- `extractor/quick_random_sample.py`
- dashboard and reporting queries
- `focused_journals_view`
- `journals_mv`

They should read from `public.crossref_current_works` or from views built on it.

### Phase 5. Deprecate the legacy raw table

After the new pipeline is validated:

- stop writing new data to `public.raw_text_data`
- keep it read-only for audit or rollback until no code depends on it

At that point the annual 2026 import becomes a normal incremental versioned ingest rather than a one-off reload.

## Implementation Order

This is the order that minimizes risk:

1. add new schema objects
2. write a new importer that streams `.jsonl.gz` into staging and upserts by DOI
3. backfill the 2025 baseline from existing data
4. migrate analysis references from `article_id` to `work_id` and `work_version_id`
5. switch readers to the new views
6. import the 2026 dump

## Success Criteria

The redesign is done when all of the following are true:

- importing a new annual dump does not require truncating `public.raw_text_data`
- one DOI maps to one stable local `work_id`
- changed Crossref metadata creates a new version row instead of a duplicate work
- existing analysis results remain attached to the work across imports
- the pipeline can tell whether the current metadata version still needs analysis
- yearly imports can be run as `languageingenetics` from `/crossref/`
