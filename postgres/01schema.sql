CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.querys (
  score  integer,
  title  text UNIQUE,
  body   text,
  answer text
);
