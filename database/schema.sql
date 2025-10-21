create table if not exists raw_text_data (id serial primary key, filesrc text);

create materialized view if not exists raw_json_data as
 with cleanup as (
  select id,
    regexp_replace(
       regexp_replace(filesrc, E'\n', ' ', 'g'),
             E'\t', '    ', 'g')
       as clean_text from raw_text_data)
  select id, clean_text::jsonb from cleanup
   where clean_text is json;

create table if not exists languageingenetics.batch_diagnostics (
    id bigserial primary key,
    batch_id int not null references languageingenetics.batches(id) on delete cascade,
    article_id int,
    event_type text not null,
    details jsonb,
    created_at timestamptz not null default current_timestamp
);

create index if not exists batch_diagnostics_batch_id_idx
    on languageingenetics.batch_diagnostics (batch_id);


