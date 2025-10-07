-- Materialized view of all journals in raw_text_data with aggregated statistics
-- This should be refreshed annually when new CrossRef data is imported
-- Run as admin user with permissions on public schema

-- Drop existing materialized view if it exists
DROP MATERIALIZED VIEW IF EXISTS public.journals_mv CASCADE;

-- Create materialized view with journal statistics
CREATE MATERIALIZED VIEW public.journals_mv AS
SELECT
    filesrc::jsonb->'container-title'->>0 AS journal_name,
    COUNT(*) AS article_count,
    MIN((filesrc::jsonb->'published'->'date-parts'->0->>0)::int) AS earliest_year,
    MAX((filesrc::jsonb->'published'->'date-parts'->0->>0)::int) AS latest_year,
    COUNT(*) FILTER (WHERE filesrc::jsonb ? 'abstract') AS articles_with_abstract,
    ROUND(100.0 * COUNT(*) FILTER (WHERE filesrc::jsonb ? 'abstract') / COUNT(*), 1) AS abstract_percentage,
    SUM((filesrc::jsonb->>'is-referenced-by-count')::int) AS total_citations,
    ROUND(AVG((filesrc::jsonb->>'is-referenced-by-count')::int), 2) AS avg_citations_per_article,
    SUM((filesrc::jsonb->>'reference-count')::int) AS total_references,
    COUNT(DISTINCT filesrc::jsonb->>'type') AS publication_types,
    -- Sample ISSNs (useful for journal identification)
    (array_agg(DISTINCT filesrc::jsonb->>'ISSN'))[1] AS sample_issn
FROM public.raw_text_data
WHERE filesrc::jsonb->'container-title' IS NOT NULL
  AND jsonb_array_length(filesrc::jsonb->'container-title') > 0
GROUP BY filesrc::jsonb->'container-title'->>0
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
