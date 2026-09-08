CREATE TABLE service_engine.scheduled_message (
    id         uuid        PRIMARY KEY,
    at         timestamptz NOT NULL,
    source     text        NOT NULL,
    subject    text        NOT NULL,
    message_id uuid        NOT NULL,
    payload    bytea       NOT NULL,
    staged_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX scheduled_message_due_idx ON service_engine.scheduled_message (at, id);
