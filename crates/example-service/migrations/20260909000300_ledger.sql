CREATE TABLE ledger_snapshot (
    id          uuid PRIMARY KEY,
    org_id      uuid   NOT NULL,
    total       bigint NOT NULL,
    last_author uuid,
    version     bigint NOT NULL
);

CREATE TABLE ledger_event (
    id      uuid   NOT NULL,
    seq     bigint NOT NULL,
    version integer NOT NULL,
    author  uuid   NOT NULL,
    payload jsonb  NOT NULL,
    PRIMARY KEY (id, seq)
);

CREATE INDEX ledger_event_author_idx ON ledger_event (author);
