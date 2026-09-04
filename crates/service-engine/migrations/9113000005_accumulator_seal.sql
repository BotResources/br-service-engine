CREATE TABLE service_engine.accumulator_seal (
    accumulator text        NOT NULL,
    key         jsonb       NOT NULL,
    high_water  bigint      NOT NULL,
    sealed_at   timestamptz NOT NULL,
    PRIMARY KEY (accumulator, key)
);

CREATE INDEX accumulator_seal_sealed_at_idx ON service_engine.accumulator_seal (sealed_at);
