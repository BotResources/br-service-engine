CREATE TABLE sample_note (
    assignment_id uuid NOT NULL,
    seq           integer NOT NULL,
    body          text NOT NULL,
    PRIMARY KEY (assignment_id, seq)
);
