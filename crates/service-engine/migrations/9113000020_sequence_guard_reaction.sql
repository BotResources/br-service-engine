ALTER TABLE service_engine.sequence_guard ADD COLUMN reaction text NOT NULL DEFAULT '';
ALTER TABLE service_engine.sequence_guard DROP CONSTRAINT sequence_guard_pkey;
ALTER TABLE service_engine.sequence_guard ADD PRIMARY KEY (producer, reaction, seq_key);
ALTER TABLE service_engine.sequence_guard ALTER COLUMN reaction DROP DEFAULT;
