ALTER TABLE service_engine.accumulator_chunk
    ADD COLUMN staged_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX accumulator_chunk_staged_at_idx ON service_engine.accumulator_chunk (staged_at);
