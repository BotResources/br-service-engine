CREATE TABLE card (
    id       uuid PRIMARY KEY,
    board_id uuid   NOT NULL,
    title    text   NOT NULL,
    status   text   NOT NULL DEFAULT 'todo',
    version  bigint NOT NULL DEFAULT 0
);

CREATE INDEX card_board_idx ON card (board_id);

CREATE TABLE card_fact (
    card    uuid   NOT NULL,
    seq     bigint NOT NULL,
    version integer NOT NULL,
    payload jsonb  NOT NULL,
    PRIMARY KEY (card, seq)
);
