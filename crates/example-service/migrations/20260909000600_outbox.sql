CREATE TABLE integration_outbox (
    id           uuid        PRIMARY KEY,
    subject      text        NOT NULL,
    payload      jsonb       NOT NULL,
    status       text        NOT NULL,
    attempts     bigint      NOT NULL DEFAULT 0,
    producer     text,
    seq_key      text,
    seq          bigint,
    last_error   text,
    published_at timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX integration_outbox_pending_idx ON integration_outbox (status, id);
