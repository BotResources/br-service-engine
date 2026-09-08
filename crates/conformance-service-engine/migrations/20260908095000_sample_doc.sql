CREATE TABLE sample_doc (
    id uuid PRIMARY KEY,
    tenant_id uuid NOT NULL,
    name text NOT NULL,
    blob_ref uuid
);
