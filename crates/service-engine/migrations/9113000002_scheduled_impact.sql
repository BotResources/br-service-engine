CREATE TABLE service_engine.scheduled_impact (
    id    uuid        PRIMARY KEY,
    at    timestamptz NOT NULL,
    noun  text        NOT NULL,
    key   jsonb       NOT NULL
);

CREATE INDEX scheduled_impact_at_idx ON service_engine.scheduled_impact (at);
