CREATE TABLE service_engine.schema_version (
    singleton        boolean PRIMARY KEY DEFAULT true,
    engine_version   text NOT NULL,
    service_version  text NOT NULL,
    pod              text NOT NULL,
    heartbeat        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT schema_version_is_singleton CHECK (singleton)
);
