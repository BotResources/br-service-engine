CREATE SCHEMA IF NOT EXISTS roster;

ALTER TABLE IF EXISTS public.known_persons SET SCHEMA roster;

CREATE TABLE IF NOT EXISTS roster.known_persons (
    user_id      uuid PRIMARY KEY,
    email        text NOT NULL,
    display_name text NOT NULL
);
