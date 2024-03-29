-- https://stackoverflow.com/a/50479342/12603110 base36
CREATE TABLE IF NOT EXISTS public.comment
(
    author text COLLATE pg_catalog."default" NOT NULL,
    author_flair_text text COLLATE pg_catalog."default",
    author_fullname text COLLATE pg_catalog."default",
    author_premium boolean,
    body text COLLATE pg_catalog."default" NOT NULL,
    comment_type text COLLATE pg_catalog."default",
    controversiality bigint NOT NULL,
    distinguished text COLLATE pg_catalog."default",
    gilded bigint NOT NULL,
    id text COLLATE pg_catalog."default" NOT NULL,
    link_id text COLLATE pg_catalog."default" NOT NULL,
    name text COLLATE pg_catalog."default",
    parent_id text COLLATE pg_catalog."default" NOT NULL,
    permalink text COLLATE pg_catalog."default",
    created_utc timestamptz NOT NULL,
    approved_at_utc timestamptz,
    author_created_utc timestamptz,
    retrieved_utc timestamptz NOT NULL,
    score bigint NOT NULL,
    stickied boolean,
    subreddit text COLLATE pg_catalog."default" NOT NULL,
    subreddit_id text COLLATE pg_catalog."default" NOT NULL,
    subreddit_type text COLLATE pg_catalog."default",
    total_awards_received bigint,
    json jsonb
);

create table json_data(json jsonb);

create or replace function json_data_trigger()
returns trigger language plpgsql as $$
begin
    insert into comment
    select
        (new.json->'author')::text,
        (new.json->'author_flair_text')::text,
        (new.json->'author_fullname')::text,
        (new.json->'author_premium')::boolean,
        (new.json->'body')::text,
        (new.json->'comment_type')::text,
        (new.json->'controversiality')::bigint,
        (new.json->'distinguished')::text,
        (new.json->'gilded')::bigint,
        (new.json->'id')::text,
        (new.json->'link_id')::text,
        (new.json->'name')::text,
        (new.json->'parent_id')::text,
        (new.json->'permalink')::text,
        to_timestamp((new.json->>'created_utc')::bigint) AT TIME ZONE 'UTC',
        to_timestamp((new.json->>'approved_at_utc')::bigint) AT TIME ZONE 'UTC',
        to_timestamp((new.json->>'author_created_utc')::bigint) AT TIME ZONE 'UTC',
        to_timestamp(COALESCE((new.json->'retrieved_utc'),(new.json->'retrieved_on'))::bigint) AT TIME ZONE 'UTC',
        (new.json->'score')::bigint,
        (new.json->'stickied')::boolean,
        (new.json->'subreddit')::text,
        (new.json->'subreddit_id')::text,
        (new.json->'subreddit_type')::text,
        (new.json->'total_awards_received')::bigint,
        nullif(new.json
        - 'author'
        - 'author_flair_text'
        - 'author_fullname'
        - 'author_premium'
        - 'body'
        - 'comment_type'
        - 'controversiality'
        - 'distinguished'
        - 'gilded'
        - 'id'
        - 'link_id'
        - 'name'
        - 'parent_id'
        - 'permalink'
        - 'created_utc'
        - 'approved_at_utc'
        - 'author_created_utc'
        - 'retrieved_utc'
        - 'retrieved_on'
        - 'score'
        - 'stickied'
        - 'subreddit'
        - 'subreddit_id'
        - 'subreddit_type'
        - 'total_awards_received', '{}');
    return NULL;
end $$;

create trigger json_data_trigger
before insert on json_data
for each row execute procedure json_data_trigger();
