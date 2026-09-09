CREATE TABLE reply (
    id        uuid PRIMARY KEY,
    board_id  uuid NOT NULL,
    org_id    uuid NOT NULL,
    text      text NOT NULL DEFAULT '',
    status    text NOT NULL DEFAULT 'streaming',
    blob_ref  uuid
);

CREATE INDEX reply_board_idx ON reply (board_id);
CREATE INDEX reply_org_idx ON reply (org_id);
