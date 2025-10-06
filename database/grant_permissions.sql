-- This script should be run by the database administrator (gregb)
-- to grant necessary permissions to the languageingenetics user

-- Grant SELECT permission on raw_text_data
GRANT SELECT ON public.raw_text_data TO languageingenetics;

-- Create a GIN index on the parsed JSON for efficient journal filtering
-- This extracts the container-title from the JSON text
-- WARNING: This will take a LONG time on a large table (hours or days)
CREATE INDEX IF NOT EXISTS idx_raw_text_data_journal
ON public.raw_text_data
USING GIN ((regexp_replace(regexp_replace(filesrc, E'\n', ' ', 'g'), E'\t', '    ', 'g')::jsonb -> 'container-title')) 
tablespace crossref_ssd;

-- Monitor index creation progress:
-- SELECT phase, blocks_done, blocks_total,
--        round(100.0 * blocks_done / nullif(blocks_total, 0), 1) AS percent_done
-- FROM pg_stat_progress_create_index;
