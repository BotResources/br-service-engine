CREATE TABLE service_engine.message_claim (
    message_id uuid        PRIMARY KEY,
    reaction   text        NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT now()
);
