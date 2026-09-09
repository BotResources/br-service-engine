ALTER TABLE service_engine.person_erasure
    ADD COLUMN manifest  jsonb       NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN purged_at  timestamptz;

CREATE INDEX person_erasure_unpurged_idx
    ON service_engine.person_erasure (erased_at)
    WHERE purged_at IS NULL;
