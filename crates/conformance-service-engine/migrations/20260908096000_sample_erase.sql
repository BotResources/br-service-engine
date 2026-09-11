CREATE TABLE sample_erase_note (
    id       uuid PRIMARY KEY,
    owner    uuid NOT NULL,
    tenant   uuid NOT NULL,
    body     text NOT NULL,
    blob_ref uuid
);

CREATE INDEX sample_erase_note_owner_idx ON sample_erase_note (owner);
CREATE INDEX sample_erase_note_tenant_idx ON sample_erase_note (tenant);

CREATE TABLE sample_erase_memo (
    id     uuid PRIMARY KEY,
    owner  uuid NOT NULL,
    tenant uuid NOT NULL,
    body   text NOT NULL
);

CREATE TABLE sample_erase_memo_fact (
    seq   bigserial PRIMARY KEY,
    memo  uuid  NOT NULL,
    owner uuid  NOT NULL,
    fact  jsonb NOT NULL
);

CREATE INDEX sample_erase_memo_owner_idx ON sample_erase_memo (owner);
CREATE INDEX sample_erase_memo_fact_owner_idx ON sample_erase_memo_fact (owner);

CREATE TABLE sample_erase_ledger_event (
    subject uuid   NOT NULL,
    seq     bigint NOT NULL,
    author  uuid   NOT NULL,
    payload jsonb  NOT NULL,
    PRIMARY KEY (subject, seq)
);

CREATE TABLE sample_erase_ledger_snapshot (
    subject uuid   PRIMARY KEY,
    tenant  uuid   NOT NULL,
    author  uuid   NOT NULL,
    total   bigint NOT NULL,
    version bigint NOT NULL
);

CREATE INDEX sample_erase_ledger_event_author_idx ON sample_erase_ledger_event (author);
