-- Move the generic Crossref incremental-import tables out of the
-- languageingenetics schema and into public with explicit crossref_* names.
--
-- Run as an admin or object owner before restarting the incremental importer.

BEGIN;

GRANT USAGE, CREATE ON SCHEMA public TO languageingenetics;

DROP VIEW IF EXISTS public.crossref_current_works;
DROP VIEW IF EXISTS languageingenetics.current_works;

DO $$
BEGIN
    IF to_regclass('languageingenetics.import_runs') IS NOT NULL THEN
        EXECUTE 'ALTER TABLE languageingenetics.import_runs SET SCHEMA public';
    END IF;
    IF to_regclass('public.import_runs') IS NOT NULL
       AND to_regclass('public.crossref_import_runs') IS NULL THEN
        EXECUTE 'ALTER TABLE public.import_runs RENAME TO crossref_import_runs';
    END IF;

    IF to_regclass('languageingenetics.works') IS NOT NULL THEN
        EXECUTE 'ALTER TABLE languageingenetics.works SET SCHEMA public';
    END IF;
    IF to_regclass('public.works') IS NOT NULL
       AND to_regclass('public.crossref_works') IS NULL THEN
        EXECUTE 'ALTER TABLE public.works RENAME TO crossref_works';
    END IF;

    IF to_regclass('languageingenetics.work_versions') IS NOT NULL THEN
        EXECUTE 'ALTER TABLE languageingenetics.work_versions SET SCHEMA public';
    END IF;
    IF to_regclass('public.work_versions') IS NOT NULL
       AND to_regclass('public.crossref_work_versions') IS NULL THEN
        EXECUTE 'ALTER TABLE public.work_versions RENAME TO crossref_work_versions';
    END IF;

    IF to_regclass('languageingenetics.legacy_raw_text_map') IS NOT NULL THEN
        EXECUTE 'ALTER TABLE languageingenetics.legacy_raw_text_map SET SCHEMA public';
    END IF;
    IF to_regclass('public.legacy_raw_text_map') IS NOT NULL
       AND to_regclass('public.crossref_legacy_raw_text_map') IS NULL THEN
        EXECUTE 'ALTER TABLE public.legacy_raw_text_map RENAME TO crossref_legacy_raw_text_map';
    END IF;

    IF to_regclass('languageingenetics.import_rejections') IS NOT NULL THEN
        EXECUTE 'ALTER TABLE languageingenetics.import_rejections SET SCHEMA public';
    END IF;
    IF to_regclass('public.import_rejections') IS NOT NULL
       AND to_regclass('public.crossref_import_rejections') IS NULL THEN
        EXECUTE 'ALTER TABLE public.import_rejections RENAME TO crossref_import_rejections';
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('public.works_normalized_doi_idx') IS NOT NULL
       AND to_regclass('public.crossref_works_normalized_doi_idx') IS NULL THEN
        EXECUTE 'ALTER INDEX public.works_normalized_doi_idx RENAME TO crossref_works_normalized_doi_idx';
    END IF;

    IF to_regclass('public.work_versions_current_idx') IS NOT NULL
       AND to_regclass('public.crossref_work_versions_current_idx') IS NULL THEN
        EXECUTE 'ALTER INDEX public.work_versions_current_idx RENAME TO crossref_work_versions_current_idx';
    END IF;

    IF to_regclass('public.work_versions_work_payload_idx') IS NOT NULL
       AND to_regclass('public.crossref_work_versions_work_payload_idx') IS NULL THEN
        EXECUTE 'ALTER INDEX public.work_versions_work_payload_idx RENAME TO crossref_work_versions_work_payload_idx';
    END IF;
END $$;

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
    v.record_type
FROM public.crossref_works w
JOIN public.crossref_work_versions v
  ON v.work_id = w.id
WHERE v.is_current;

COMMIT;
