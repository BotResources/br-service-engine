CREATE TABLE sample_widget (
    id        uuid PRIMARY KEY,
    tenant_id uuid    NOT NULL,
    label     text    NOT NULL,
    closed    boolean NOT NULL DEFAULT false
);

CREATE INDEX sample_widget_tenant_idx ON sample_widget (tenant_id);
