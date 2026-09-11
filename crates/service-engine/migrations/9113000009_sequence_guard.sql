CREATE TABLE service_engine.sequence_guard (
    producer text   NOT NULL,
    seq_key  text   NOT NULL,
    last_seq bigint NOT NULL,
    PRIMARY KEY (producer, seq_key)
);
