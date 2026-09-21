ALTER TABLE service_engine.blob
    ADD COLUMN expected_size bigint,
    ADD COLUMN expected_sha256 bytea,
    ADD COLUMN etag text,
    ADD COLUMN sha256 bytea,
    ADD COLUMN failed_reason text,
    ADD COLUMN failed_at timestamptz;

CREATE INDEX blob_failed_idx ON service_engine.blob (kind, failed_at) WHERE state = 'failed';
