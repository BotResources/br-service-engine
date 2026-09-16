-- Additive: existing installations keep their watermark rows. NULL means no
-- stream identity has been adopted yet; the next full read adopts the one it
-- reads. Text preserves the broker's nanosecond creation timestamp, which a
-- PostgreSQL timestamptz would round.
ALTER TABLE service_engine.mirror_watermark ADD COLUMN stream_created_at text;
