-- Schema support for semantic Crossref imports.
--
-- This migration is safe to run before restarting the annual importer. The
-- fallback identity index is built concurrently because crossref_works is large.

ALTER TABLE public.crossref_import_runs
    ADD COLUMN IF NOT EXISTS max_publication_date DATE;
ALTER TABLE public.crossref_import_runs
    ADD COLUMN IF NOT EXISTS max_publication_year INT;

ALTER TABLE public.crossref_works
    ADD COLUMN IF NOT EXISTS fallback_identity TEXT;

ALTER TABLE public.crossref_work_versions
    ADD COLUMN IF NOT EXISTS pub_date DATE;
ALTER TABLE public.crossref_work_versions
    ADD COLUMN IF NOT EXISTS title_norm TEXT;
ALTER TABLE public.crossref_work_versions
    ADD COLUMN IF NOT EXISTS abstract_norm TEXT;
ALTER TABLE public.crossref_work_versions
    ADD COLUMN IF NOT EXISTS text_fingerprint TEXT;

CREATE TABLE IF NOT EXISTS public.crossref_work_text_changes (
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

CREATE INDEX CONCURRENTLY IF NOT EXISTS crossref_works_fallback_identity_hash_idx
    ON public.crossref_works USING hash (fallback_identity)
    WHERE fallback_identity IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS crossref_work_text_changes_to_version_idx
    ON public.crossref_work_text_changes (to_work_version_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS crossref_work_text_changes_from_version_idx
    ON public.crossref_work_text_changes (from_work_version_id);

-- Cover the annual import's DOI mapping query:
--   import_stage.normalized_doi -> crossref_works.id.
-- The original unique DOI index enforces identity but does not carry the id
-- column, forcing random heap reads on a large table.
CREATE INDEX CONCURRENTLY IF NOT EXISTS crossref_works_normalized_doi_id_idx
    ON public.crossref_works (normalized_doi) INCLUDE (id)
    WHERE normalized_doi IS NOT NULL;

-- Cover current-corpus cache scans that start from crossref_work_versions.work_id
-- and only need the matching DOI from crossref_works.
CREATE INDEX CONCURRENTLY IF NOT EXISTS crossref_works_id_normalized_doi_idx
    ON public.crossref_works (id) INCLUDE (normalized_doi)
    WHERE normalized_doi IS NOT NULL;

-- Cover the annual import's unchanged-text check:
--   work_id -> current version id/import_run/text_fingerprint.
-- This avoids fetching title/abstract/raw rows from the large versions heap for
-- records whose semantic text has not changed.
CREATE INDEX CONCURRENTLY IF NOT EXISTS crossref_work_versions_current_fingerprint_idx
    ON public.crossref_work_versions (work_id)
    INCLUDE (id, import_run_id, text_fingerprint)
    WHERE is_current;

-- The unique constraint on (work_id, payload_sha256) already provides this
-- lookup. Keeping the duplicate non-unique index adds about 33 GB of index
-- maintenance to every version-row write.
DROP INDEX CONCURRENTLY IF EXISTS public.crossref_work_versions_work_payload_idx;

CREATE OR REPLACE VIEW public.crossref_current_works AS
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
