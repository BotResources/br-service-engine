CREATE TABLE service_engine.leader_slot (
    name         text        NOT NULL,
    slot         timestamptz NOT NULL,
    pod          text        NOT NULL,
    lease_until  timestamptz NOT NULL,
    completed_at timestamptz,
    PRIMARY KEY (name, slot)
);

CREATE INDEX leader_slot_sweep_idx ON service_engine.leader_slot (completed_at, slot);
