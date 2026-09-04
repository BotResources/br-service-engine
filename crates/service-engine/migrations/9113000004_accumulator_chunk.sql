CREATE TABLE service_engine.accumulator_chunk (
    accumulator text   NOT NULL,
    key         jsonb  NOT NULL,
    seq         bigint NOT NULL,
    chunk       jsonb  NOT NULL,
    PRIMARY KEY (accumulator, key, seq)
);
