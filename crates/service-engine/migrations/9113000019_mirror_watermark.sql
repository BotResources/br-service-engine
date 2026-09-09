CREATE TABLE service_engine.mirror_watermark (
    mirror   text   NOT NULL,
    bucket   text   NOT NULL,
    revision bigint NOT NULL,
    PRIMARY KEY (mirror, bucket)
);
