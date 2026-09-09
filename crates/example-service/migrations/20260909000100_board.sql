CREATE TABLE board (
    id        uuid PRIMARY KEY,
    org_id    uuid    NOT NULL,
    name      text    NOT NULL,
    is_public boolean NOT NULL DEFAULT false,
    state     text    NOT NULL DEFAULT 'active'
);

CREATE INDEX board_org_idx ON board (org_id);

ALTER TABLE board ENABLE ROW LEVEL SECURITY;
ALTER TABLE board FORCE ROW LEVEL SECURITY;

CREATE POLICY board_org ON board
    USING (
        nullif(current_setting('app.current_org_id', true), '') IS NULL
        OR org_id = nullif(current_setting('app.current_org_id', true), '')::uuid
        OR is_public
    );

CREATE TABLE board_member (
    board_id uuid NOT NULL,
    user_id  uuid NOT NULL,
    PRIMARY KEY (board_id, user_id)
);

CREATE INDEX board_member_user_idx ON board_member (user_id);
