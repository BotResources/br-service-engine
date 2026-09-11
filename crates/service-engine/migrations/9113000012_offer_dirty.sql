CREATE SEQUENCE service_engine.offer_dirty_seq;

CREATE TABLE service_engine.offer_dirty (
    offer   text   NOT NULL,
    kv_key  text   NOT NULL,
    agg_key bytea,
    seq     bigint NOT NULL,
    PRIMARY KEY (offer, kv_key)
);

CREATE INDEX offer_dirty_drain_idx ON service_engine.offer_dirty (offer, seq);
