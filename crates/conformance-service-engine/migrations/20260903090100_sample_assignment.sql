CREATE TABLE sample_assignment (
    id        uuid PRIMARY KEY,
    tenant_id uuid    NOT NULL,
    title     text    NOT NULL,
    closed    boolean NOT NULL DEFAULT false
);

CREATE INDEX sample_assignment_tenant_idx ON sample_assignment (tenant_id);

ALTER TABLE sample_assignment ENABLE ROW LEVEL SECURITY;
ALTER TABLE sample_assignment FORCE ROW LEVEL SECURITY;

CREATE POLICY sample_assignment_tenant ON sample_assignment
    USING (tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid);
