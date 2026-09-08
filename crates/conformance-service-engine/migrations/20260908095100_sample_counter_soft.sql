CREATE TABLE sample_counter_soft (
    id      uuid    PRIMARY KEY,
    tenant  uuid    NOT NULL,
    total   bigint  NOT NULL,
    closed  boolean NOT NULL DEFAULT false,
    version bigint  NOT NULL DEFAULT 0
);

CREATE INDEX sample_counter_soft_tenant_idx ON sample_counter_soft (tenant);

CREATE TABLE sample_counter_soft_fact (
    counter uuid        NOT NULL,
    seq     bigint      NOT NULL,
    version integer     NOT NULL,
    payload jsonb       NOT NULL,
    at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (counter, seq),
    CONSTRAINT sample_counter_soft_fact_ceiling
        CHECK ((payload ->> 'amount') IS NULL OR (payload ->> 'amount')::bigint <= 100)
);
