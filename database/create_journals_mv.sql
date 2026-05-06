-- Materialized view of all current Crossref journals with aggregated statistics
-- This should be refreshed annually when new CrossRef data is imported
-- Run as admin user with permissions on public schema

-- Drop existing materialized view if it exists
DROP MATERIALIZED VIEW IF EXISTS public.journals_mv CASCADE;

-- Create materialized view with journal statistics
CREATE MATERIALIZED VIEW public.journals_mv tablespace crossref_space as SELECT
    journal_name,
    COUNT(*) AS article_count,
    MIN(pub_year) AS earliest_year,
    MAX(pub_year) AS latest_year,
    COUNT(*) FILTER (WHERE abstract IS NOT NULL) AS articles_with_abstract,
    ROUND(100.0 * COUNT(*) FILTER (WHERE abstract IS NOT NULL) / COUNT(*), 1) AS abstract_percentage,
    NULL::numeric AS total_citations,
    NULL::numeric AS avg_citations_per_article,
    NULL::bigint AS total_references,
    COUNT(DISTINCT record_type) AS publication_types,
    NULL::text AS sample_issn
FROM public.crossref_current_works
WHERE journal_name IS NOT NULL
GROUP BY journal_name
HAVING COUNT(*) > 10
ORDER BY article_count DESC;

-- Create unique index on journal_name for fast lookups
CREATE UNIQUE INDEX idx_journals_mv_name ON public.journals_mv(journal_name);

-- Create additional index on article_count for sorting by size
CREATE INDEX idx_journals_mv_count ON public.journals_mv(article_count DESC);

-- Grant SELECT permission to languageingenetics user
GRANT SELECT ON public.journals_mv TO languageingenetics;

-- To refresh the materialized view (run annually after CrossRef data import):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY public.journals_mv;

-- Example queries:
-- List top journals by article count:
--   SELECT journal_name, article_count, earliest_year, latest_year FROM public.journals_mv ORDER BY article_count DESC LIMIT 20;
--
-- Find journals with good abstract coverage:
--   SELECT journal_name, article_count, abstract_percentage FROM public.journals_mv WHERE abstract_percentage > 90 ORDER BY article_count DESC;
--
-- Search for genetics journals:
--   SELECT journal_name, article_count, earliest_year, latest_year FROM public.journals_mv WHERE journal_name ILIKE '%genetic%' ORDER BY article_count DESC;
