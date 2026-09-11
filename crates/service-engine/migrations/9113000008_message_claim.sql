CREATE TABLE service_engine.message_claim (
    message_id uuid        NOT NULL,
    reaction   text        NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, reaction)
);
