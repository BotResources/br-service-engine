CREATE TABLE IF NOT EXISTS service_engine.person_erasure (
    person_id UUID PRIMARY KEY,
    erased_at TIMESTAMPTZ NOT NULL
);
