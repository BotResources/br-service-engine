CREATE TABLE sample_erase_secret (
    id     uuid PRIMARY KEY,
    owner  uuid NOT NULL,
    tenant uuid NOT NULL,
    body   text NOT NULL
);

CREATE INDEX sample_erase_secret_owner_idx ON sample_erase_secret (owner);

ALTER TABLE sample_erase_secret ENABLE ROW LEVEL SECURITY;
ALTER TABLE sample_erase_secret FORCE ROW LEVEL SECURITY;

CREATE POLICY sample_erase_secret_strict ON sample_erase_secret
    USING (
        tenant = nullif(current_setting('app.current_tenant_id', true), '')::uuid
        OR current_setting('app.erasing', true) = 'on'
    );
