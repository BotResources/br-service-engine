CREATE TABLE service_engine.dead_letter (
    id         uuid        PRIMARY KEY,
    source     text        NOT NULL,
    reaction   text        NOT NULL,
    subject    text        NOT NULL,
    message_id uuid        NOT NULL,
    payload    bytea       NOT NULL,
    producer   text,
    seq_key    text,
    seq        bigint,
    error      text        NOT NULL,
    delivered  integer     NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (reaction, message_id)
);

CREATE INDEX dead_letter_source_idx ON service_engine.dead_letter (source, first_seen);
