CREATE TABLE service_engine.event_log (
    noun    text    NOT NULL,
    key     text    NOT NULL,
    seq     bigint  NOT NULL,
    version integer NOT NULL,
    payload jsonb   NOT NULL,
    PRIMARY KEY (noun, key, seq)
);

CREATE TABLE service_engine.event_snapshot (
    noun    text   NOT NULL,
    key     text   NOT NULL,
    version bigint NOT NULL,
    state   jsonb  NOT NULL,
    PRIMARY KEY (noun, key)
);
