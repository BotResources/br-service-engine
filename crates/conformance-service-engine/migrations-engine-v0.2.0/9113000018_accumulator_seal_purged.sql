ALTER TABLE service_engine.accumulator_seal
    ADD COLUMN purged_at timestamptz;

CREATE INDEX accumulator_seal_unpurged_idx
    ON service_engine.accumulator_seal (accumulator, key)
    WHERE purged_at IS NULL;
