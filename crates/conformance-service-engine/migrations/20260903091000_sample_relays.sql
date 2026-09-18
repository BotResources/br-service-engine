CREATE TABLE sample_relay_row (
    id         uuid        PRIMARY KEY,
    claimed_by text,
    claimed_at timestamptz
);

CREATE INDEX sample_relay_row_unclaimed_idx ON sample_relay_row (claimed_at NULLS FIRST, id);

CREATE TABLE sample_relay_claim (
    id         uuid PRIMARY KEY,
    row_id     uuid NOT NULL,
    claimed_by text NOT NULL
);

CREATE TABLE sample_kv_pending (
    key        text        PRIMARY KEY,
    version    bigint      NOT NULL,
    label      text,
    applied_at timestamptz
);

CREATE TABLE sample_leader_run (
    id   uuid        PRIMARY KEY,
    slot timestamptz NOT NULL,
    pod  text        NOT NULL,
    at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sample_staged_impact (
    id      uuid  PRIMARY KEY,
    payload jsonb NOT NULL
);
