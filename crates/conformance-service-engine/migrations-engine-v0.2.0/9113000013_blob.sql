CREATE TABLE service_engine.blob (
    id uuid PRIMARY KEY,
    object_key text NOT NULL,
    kind text NOT NULL,
    service text NOT NULL,
    content_type text NOT NULL,
    file_name text NOT NULL,
    owner_id uuid,
    size bigint,
    state text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    uploaded_at timestamptz,
    detached_at timestamptz
);

CREATE INDEX blob_reaper_idx ON service_engine.blob (kind, state);
CREATE INDEX blob_owner_idx ON service_engine.blob (owner_id) WHERE owner_id IS NOT NULL;
