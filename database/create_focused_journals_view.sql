-- Create a view for querying articles from enabled (focused) journals
-- This combines raw article data, analysis results, and journal enablement status

CREATE OR REPLACE VIEW languageingenetics.focused_journals_view AS
SELECT
    -- Article identifiers
    r.id AS article_id,

    -- Journal information
    (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'container-title' ->> 0) AS journal_name,
    j.id AS journal_table_id,
    j.enabled AS journal_enabled,

    -- Article metadata from CrossRef
    (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'DOI') AS doi,
    (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'title') AS title,
    ((regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'published' -> 'date-parts' -> 0 ->> 0)::integer) AS pub_year,
    (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'abstract') AS abstract,
    (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb ->> 'type') AS article_type,

    -- Analysis results
    f.id AS analysis_id,
    f.processed AS is_processed,
    f.has_abstract,
    f.when_processed,
    f.caucasian,
    f.white,
    f.european,
    f.european_phrase_used,
    f.other,
    f.other_phrase_used,

    -- Token usage
    f.prompt_tokens,
    f.completion_tokens,

    -- Batch information
    f.batch_id

FROM public.raw_text_data r
INNER JOIN languageingenetics.journals j
    ON (regexp_replace(regexp_replace(r.filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'container-title' ->> 0) = j.name
LEFT JOIN languageingenetics.files f
    ON r.id = f.article_id
WHERE j.enabled = true;

-- Grant access to the languageingenetics user
GRANT SELECT ON languageingenetics.focused_journals_view TO languageingenetics;

-- Add a helpful comment
COMMENT ON VIEW languageingenetics.focused_journals_view IS
'View of articles from enabled journals with their analysis results. Filters to only journals marked as enabled in languageingenetics.journals table.';
