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


