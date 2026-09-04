CREATE TABLE service_engine.kv_relay_watermark (
    relay   text   NOT NULL,
    kv_key  text   NOT NULL,
    version bigint NOT NULL,
    PRIMARY KEY (relay, kv_key)
);
