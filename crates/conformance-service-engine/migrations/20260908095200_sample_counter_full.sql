CREATE TABLE sample_counter_full_event (
    counter uuid    NOT NULL,
    seq     bigint  NOT NULL,
    version integer NOT NULL,
    payload jsonb   NOT NULL,
    PRIMARY KEY (counter, seq)
);

CREATE TABLE sample_counter_full_snapshot (
    id          uuid    PRIMARY KEY,
    tenant      uuid    NOT NULL,
    total       bigint  NOT NULL,
    closed      boolean NOT NULL DEFAULT false,
    last_author text,
    version     bigint  NOT NULL,
    CONSTRAINT sample_counter_full_ceiling CHECK (total <= 100)
);

CREATE INDEX sample_counter_full_snapshot_tenant_idx ON sample_counter_full_snapshot (tenant);
