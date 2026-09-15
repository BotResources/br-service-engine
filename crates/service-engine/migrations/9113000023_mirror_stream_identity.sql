-- Additive: old installations retain their watermark rows. NULL means that no
-- stream identity has been adopted yet; the next successful snapshot adopts it.
-- Text preserves the broker's nanosecond creation timestamp without PG rounding.
ALTER TABLE service_engine.mirror_watermark ADD COLUMN stream_created_at text;
