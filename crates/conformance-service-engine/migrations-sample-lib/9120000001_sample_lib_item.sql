CREATE SCHEMA IF NOT EXISTS sample_lib;

CREATE TABLE IF NOT EXISTS sample_lib.item (
    id uuid PRIMARY KEY,
    label text NOT NULL
);
