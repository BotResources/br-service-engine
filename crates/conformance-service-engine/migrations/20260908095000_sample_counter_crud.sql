CREATE TABLE sample_counter_crud (
    id     uuid    PRIMARY KEY,
    tenant uuid    NOT NULL,
    total  bigint  NOT NULL,
    closed boolean NOT NULL DEFAULT false
);

CREATE INDEX sample_counter_crud_tenant_idx ON sample_counter_crud (tenant);
