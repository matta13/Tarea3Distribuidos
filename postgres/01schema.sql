CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.querys (
  score  integer,
  title  text UNIQUE,
  body   text,
  answer text
);

CREATE TABLE IF NOT EXISTS public.yahoo_dataset (
    score TEXT,
    title TEXT,
    body TEXT,
    answer TEXT
);

CREATE TABLE IF NOT EXISTS public.llm_dataset (
    score TEXT,
    title TEXT,
    body TEXT,
    answer TEXT
);

COPY public.yahoo_dataset FROM '/data_import/train_10k.csv' DELIMITER ',' CSV HEADER;
COPY public.llm_dataset FROM '/data_import/train_10k_actualizado.csv' DELIMITER ',' CSV HEADER;
