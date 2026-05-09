# Incremental Crossref Import Redesign

Status: revised after the aborted 2026 import; importer input updated to SQLite staging

This document describes the import redesign needed to support yearly Crossref additions without destroying existing data or breaking downstream analysis.

Current implementation note: `crossrefimport` no longer streams annual
`*.jsonl.gz` files directly. The annual dump is still read by prefilter tools,
but the importer reads a bounded SQLite staging database produced by
`crossrefclassify` or `crossreffilter`.

## April 2026 Correction: Versioning Should Be Semantic, Not Raw-JSON Based

The first attempted 2026 annual import exposed a design bug in the original versioning model. The importer treated `sha256(raw_json_text)` as the version identity. Crossref changes volatile fields such as `indexed`, `deposited`, citation counts, links, and other bookkeeping across annual dumps, so that rule created a near-one-new-version-per-row explosion even when the title, abstract, journal, publication year, and record type were unchanged.

For this project, raw JSON churn is not a meaningful metadata version. The meaningful question is whether the text that feeds analysis changed, especially title or abstract. Therefore yearly imports should not append a full `crossref_work_versions` row just because the raw payload changed.

The revised target is:

- keep one stable `public.crossref_works` row per DOI/work
- keep a current metadata/raw payload row for the latest analysis-relevant Crossref state
- store small history rows only when analysis-relevant text changes
- use a semantic fingerprint, not raw JSON bytes, to decide whether a work needs re-analysis
- use DOI first, then a normalized journal/title/abstract fallback identity for records without a DOI or records whose DOI has newly appeared
- treat the Crossref dump files themselves as the recoverable archive of raw annual payloads

A practical semantic fingerprint for this project is based on normalized title and abstract. If dashboard membership should also update from imports, keep journal name, publication year, and record type as current metadata fields, but do not let changes in `indexed`, `deposited`, citation counts, or Crossref bookkeeping create analysis versions.

Implementation note: `public.crossref_work_versions.payload_sha256` remains a raw-payload checksum for compatibility and integrity checks, but it is no longer used as the semantic version identity. The importer compares normalized title/abstract text and records rows in `public.crossref_work_text_changes` only when title or abstract changed. If only Crossref bookkeeping changed, it must not rewrite the current `crossref_work_versions` row; doing so rewrites hundreds of GB for no analysis value.

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
- prefilter tools must explicitly whitelist `*.jsonl.gz` before writing SQLite
  staging records
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

## Revised Data Model

The original design separated three concerns:

- one stable local work row
- one version row per changed metadata payload
- one import run row per snapshot ingest

That is still directionally right, but "changed metadata payload" must not mean raw JSON byte changes. The version/history layer should be about analysis-relevant extracted metadata.

### 1. Import Runs

Create a run table to record each ingest attempt. These Crossref corpus tables are generic infrastructure, so they should live in `public` with explicit `crossref_` names rather than in `languageingenetics`.

```sql
CREATE TABLE public.crossref_import_runs (
    id BIGSERIAL PRIMARY KEY,
    run_label TEXT NOT NULL UNIQUE,
    source_type TEXT NOT NULL,          -- annual_dump, monthly_snapshot, rest_delta
    source_path TEXT NOT NULL,
    snapshot_date DATE,
    max_publication_date DATE,
    max_publication_year INT,
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
    fallback_identity TEXT,
    first_import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    latest_import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX crossref_works_normalized_doi_idx
    ON public.crossref_works(normalized_doi)
    WHERE normalized_doi IS NOT NULL;

CREATE INDEX CONCURRENTLY crossref_works_fallback_identity_hash_idx
    ON public.crossref_works USING hash (fallback_identity)
    WHERE fallback_identity IS NOT NULL;

CREATE INDEX CONCURRENTLY crossref_works_normalized_doi_id_idx
    ON public.crossref_works (normalized_doi) INCLUDE (id)
    WHERE normalized_doi IS NOT NULL;

CREATE INDEX CONCURRENTLY crossref_works_id_normalized_doi_idx
    ON public.crossref_works (id) INCLUDE (normalized_doi)
    WHERE normalized_doi IS NOT NULL;
```

Rules:

- `id` is the primary key used by downstream tables.
- `normalized_doi` is the preferred stable identifier when present.
- store DOI in a normalized form, for example lowercase and trimmed
- do not use Crossref row position or file number as a durable key
- do not make DOI the primary key because missing-DOI edge cases should not break the schema
- `fallback_identity` is the normalized journal/title/abstract tuple used only for exact identity fallback
- use PostgreSQL's `hash` index for equality lookup on that fallback identity; do not btree-index a digest for this purpose
- reject no-DOI records only when they also lack enough journal/title/abstract text to form a fallback identity
- if a later Crossref record supplies a DOI for an existing fallback match, update the stable work row with that DOI

### 3. Current Metadata And Text History

The existing `public.crossref_work_versions` table can continue to serve as the current metadata source during the migration, but it should not keep accumulating rows for Crossref bookkeeping churn. A cleaner long-term shape is a current row plus a small text history table.

```sql
CREATE TABLE public.crossref_work_current (
    work_id BIGINT PRIMARY KEY REFERENCES public.crossref_works(id) ON DELETE CASCADE,
    latest_import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    raw_json_text TEXT NOT NULL,
    title TEXT,
    abstract TEXT,
    title_abstract_sha256 TEXT NOT NULL,
    journal_name TEXT,
    pub_year INT,
    record_type TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.crossref_work_text_history (
    id BIGSERIAL PRIMARY KEY,
    work_id BIGINT NOT NULL REFERENCES public.crossref_works(id) ON DELETE CASCADE,
    import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    title TEXT,
    abstract TEXT,
    title_abstract_sha256 TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (work_id, title_abstract_sha256)
);
```

Rules:

- Every import is compared against the current row for the same work.
- If normalized title and abstract are unchanged, leave the current version row alone; do not refresh raw JSON merely because Crossref bookkeeping fields changed, do not create a history row, and do not queue re-analysis.
- If normalized title or abstract changed, add a `crossref_work_text_history` row and mark the work as needing analysis against the new text hash.
- Extract a few current fields into plain columns so the dashboard and batch queries do not keep reparsing the whole JSON blob.
- Do not version on volatile fields such as `indexed`, `deposited`, `is-referenced-by-count`, `reference`, `link`, or Crossref bookkeeping fields.

Compatibility note: while the deployed schema still has `public.crossref_work_versions`, the importer should use semantic comparison of extracted fields rather than `raw_json_text` hashes. A later compaction can rename or replace the table once downstream readers are stable.

The deployed compatibility table currently carries these additional columns:

- `pub_date`: extracted publication date when Crossref provides date-parts
- `title_norm`, `abstract_norm`: normalized comparison text
- `text_fingerprint`: SHA-256 over normalized title and abstract, used for cheap equality checks and re-analysis decisions

`text_fingerprint` is intentionally nullable because it was added after the 2025 legacy backfill. Do not backfill it in-place across the large legacy `crossref_work_versions` heap. For full annual snapshot prefilters, compute missing legacy fingerprints while building the SQLite DOI cache described in [crossref_sqlite_cache_pipeline.md](crossref_sqlite_cache_pipeline.md), and keep storing inline fingerprints only for new or genuinely changed semantic versions going forward.

The import path should also have a covering current-version index so unchanged rows can be checked without fetching title, abstract, or raw JSON from the large heap:

```sql
CREATE INDEX CONCURRENTLY crossref_work_versions_current_fingerprint_idx
ON public.crossref_work_versions (work_id)
INCLUDE (id, import_run_id, text_fingerprint)
WHERE is_current;
```

For cache-building or classification prefilters that scan current versions first, the companion `crossref_works_id_normalized_doi_idx` index avoids fetching `crossref_works` heap pages just to recover `normalized_doi`.

The small change table is:

```sql
CREATE TABLE public.crossref_work_text_changes (
    id BIGSERIAL PRIMARY KEY,
    work_id BIGINT NOT NULL REFERENCES public.crossref_works(id) ON DELETE CASCADE,
    from_work_version_id BIGINT REFERENCES public.crossref_work_versions(id) ON DELETE SET NULL,
    to_work_version_id BIGINT NOT NULL REFERENCES public.crossref_work_versions(id) ON DELETE CASCADE,
    from_import_run_id BIGINT REFERENCES public.crossref_import_runs(id),
    to_import_run_id BIGINT NOT NULL REFERENCES public.crossref_import_runs(id),
    previous_title TEXT,
    previous_abstract TEXT,
    new_title TEXT,
    new_abstract TEXT,
    previous_title_norm TEXT,
    previous_abstract_norm TEXT,
    new_title_norm TEXT,
    new_abstract_norm TEXT,
    previous_text_fingerprint TEXT,
    new_text_fingerprint TEXT,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (work_id, from_work_version_id, to_work_version_id)
);
```

The canonical database raw JSON location remains `public.crossref_work_versions.raw_json_text`, but it is canonical for the stored semantic version, not for every annual dump's byte-for-byte payload. The annual dump files are the recoverable archive for unchanged raw bookkeeping churn; changed title/abstract records get a new current row plus a compact change record.

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

- malformed JSON
- missing DOI plus missing fallback identity (`missing_doi_identity`)
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
    v.record_type,
    v.pub_date
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

- stream SQLite staging records produced from the `.jsonl.gz` snapshot
- parse the DOI once
- compute a raw payload hash once for integrity
- compute normalized title/abstract and the semantic text fingerprint
- compute fallback identity from normalized journal/title/abstract when available
- load into a staging table for the current run

Suggested staging shape:

```sql
CREATE TEMP TABLE import_stage (
    normalized_doi TEXT,
    original_doi TEXT,
    raw_json_text TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    text_fingerprint TEXT NOT NULL,
    title TEXT,
    abstract TEXT,
    title_norm TEXT NOT NULL,
    abstract_norm TEXT NOT NULL,
    journal_name TEXT,
    pub_year INT,
    pub_date DATE,
    fallback_identity TEXT,
    record_type TEXT
);
```

This keeps the import resumable and lets SQL handle the final merges in batches.

### Step 3. Resolve stable works

Resolve each staged row in this order:

- match by `normalized_doi` when the DOI already exists
- otherwise, if the publication date/year is not clearly after the previous completed dump cutoff, match by exact `fallback_identity`
- if the fallback match has no DOI and the new record has one, update the existing work with the new DOI
- if the record is clearly newer than the previous dump cutoff, or no fallback match exists, insert a new `public.crossref_works` row
- reject only rows that have neither DOI nor enough journal/title/abstract text to build `fallback_identity`

### Step 4. Insert only changed versions

For each resolved work:

- compare normalized title and abstract against the current row for that work
- when the current row is a legacy backfill row with missing normalized text/fingerprint columns, compute the current row's normalized title/abstract text on demand before deciding it changed
- if unchanged, do not insert a history/version row
- do not refresh the current row's raw JSON or metadata for unchanged records, because that turns a semantic no-op into a full heap/index/WAL rewrite
- if changed, insert a new current `crossref_work_versions` row and add a compact `crossref_work_text_changes` audit row
- use `text_fingerprint`, not `payload_sha256`, to decide whether re-analysis may be needed

### Step 5. Mark snapshot visibility

Do not update every work just to record that it appeared in the newest full snapshot; that creates the same full-table rewrite problem as refreshing unchanged raw JSON.

Minimum useful state:

- `latest_import_run_id` in `crossref_works` for newly inserted or semantically changed works
- a current metadata row keyed by `work_id`

Optional later addition:

- a `work_run_presence(work_id, import_run_id)` table if we need exact "present in snapshot X" reporting

### Step 6. Queue re-analysis only when needed

If the current version changed in a way that affects title or abstract, the work should be eligible for a fresh OpenAI batch submission.

A simple rule is enough for the first pass:

- if `text_fingerprint` changed and that text hash has not been analyzed, it is pending

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
- `public.crossref_work_text_changes`
- `public.crossref_legacy_raw_text_map`
- `public.crossref_import_rejections`
- `public.crossref_current_works` view

Do this without removing `public.raw_text_data`.

The compatibility migration is captured in `database/migrate_crossref_semantic_import.sql`. Run it before restarting a stopped annual import so the large fallback identity index is built with `CREATE INDEX CONCURRENTLY`.

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
2. write a new importer that reads SQLite staging records and upserts by DOI
3. backfill the 2025 baseline from existing data
4. migrate analysis references from `article_id` to `work_id` and `work_version_id`
5. switch readers to the new views
6. import the 2026 dump

## Success Criteria

The redesign is done when all of the following are true:

- importing a new annual dump does not require truncating `public.raw_text_data`
- one DOI maps to one stable local `work_id`
- changed title or abstract creates a small history row instead of a duplicate work
- Crossref bookkeeping churn does not create a new DB version
- existing analysis results remain attached to the work across imports
- the pipeline can tell whether the current metadata version still needs analysis
- yearly imports can be run as `languageingenetics` from `/crossref/`

## Follow-up: Retractions And Corrections

After the 2025 backfill is complete, do a focused pass on retraction-style metadata before finalizing any schema for it.

### Investigation Plan

1. Find a small set of known 2025-era retractions, corrections, errata, and withdrawals in scope for this project.
2. Check how those records appear in the current Crossref payloads stored in `public.crossref_work_versions.raw_json_text`.
3. Record which raw keys actually carry the useful signal in practice.
   Likely candidates include `update-to`, `update-policy`, `relation`, Crossmark-related fields, and `assertion`, but this must be verified from real 2025 examples rather than assumed from older samples.

### Schema Plan

Retraction/correction state should be stored on `public.crossref_work_versions`, not only on `public.crossref_works`, because this is metadata that can change across imports.

The exact extracted column set is intentionally deferred until the real 2025 examples are checked, but the likely shape is:

- a compact extracted status column such as `retraction_status` or `update_type`
- possibly one or more helper columns such as `update_target_doi` or `has_retraction_signal`

Whatever final shape is chosen, expose the current status through `public.crossref_current_works` as well.

### Index Plan

Once the extracted column is defined, add a partial index so that retracted or corrected current works can be found cheaply. For example:

```sql
CREATE INDEX crossref_work_versions_retraction_idx
ON public.crossref_work_versions(work_id)
WHERE is_current AND retraction_status IS NOT NULL;
```

The exact predicate should match the final extracted-column design.

### Project-Specific Analysis

For this project, once retraction/correction flags exist, test whether the language-use patterns differ between retracted and non-retracted papers.

At minimum, compare:

- counts and rates of the existing terminology flags in `languageingenetics.files`
- journal mix and publication-year mix, so obvious composition effects are not mistaken for a language effect
