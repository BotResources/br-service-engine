CREATE TABLE sample_assignment_link (
    assignment_id uuid NOT NULL,
    foreign_key   text NOT NULL,
    PRIMARY KEY (assignment_id, foreign_key)
);

CREATE INDEX sample_assignment_link_fk_idx ON sample_assignment_link (foreign_key);
