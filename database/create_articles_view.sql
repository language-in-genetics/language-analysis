-- Create a flattened view of articles from raw_text_data
-- This makes it easy to query by journal, title, year, etc.

-- First, create indexes on the underlying table for efficient queries
-- These indexes use functional expressions on the JSONB-cast filesrc column

-- Index for journal queries (container-title is an array, so we index the first element)
CREATE INDEX IF NOT EXISTS idx_raw_text_data_journal_title
ON public.raw_text_data
USING GIN (((regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'container-title')));

-- Index for publication year queries
CREATE INDEX IF NOT EXISTS idx_raw_text_data_pub_year
ON public.raw_text_data
((regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'published' -> 'date-parts' -> 0 -> 0));

-- Index for DOI lookups
CREATE INDEX IF NOT EXISTS idx_raw_text_data_doi
ON public.raw_text_data
((regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'DOI'));

-- Create the flattened view
CREATE OR REPLACE VIEW public.articles AS
SELECT
    id,
    (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'DOI') AS doi,
    (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'container-title' ->> 0) AS journal,
    (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'title' ->> 0) AS title,
    (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'abstract') AS abstract,
    ((regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'published' -> 'date-parts' -> 0 -> 0)::text)::integer AS pub_year,
    (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'type') AS type,
    (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'publisher') AS publisher,
    (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'author') AS authors,
    (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'URL') AS url,
    (regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'subject') AS subjects
FROM public.raw_text_data;

-- Grant SELECT permission to languageingenetics user
GRANT SELECT ON public.articles TO languageingenetics;

-- Show some example queries you can now run:
-- SELECT count(*) FROM public.articles WHERE journal = 'Familial Cancer';
-- SELECT title, pub_year FROM public.articles WHERE journal = 'Familial Cancer' AND pub_year >= 2020;
-- SELECT journal, count(*) FROM public.articles GROUP BY journal ORDER BY count DESC;
-- SELECT * FROM public.articles WHERE abstract IS NOT NULL AND journal = 'Familial Cancer' LIMIT 10;
