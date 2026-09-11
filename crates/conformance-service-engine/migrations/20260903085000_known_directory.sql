CREATE TABLE known_users (
    user_id    uuid PRIMARY KEY,
    email      text NOT NULL,
    first_name text,
    last_name  text,
    extensions jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE known_groups (
    group_id uuid PRIMARY KEY,
    name     text NOT NULL
);

CREATE TABLE known_user_group (
    group_id uuid NOT NULL REFERENCES known_groups (group_id) ON DELETE CASCADE,
    user_id  uuid NOT NULL,
    PRIMARY KEY (group_id, user_id)
);

CREATE INDEX known_user_group_user_idx ON known_user_group (user_id);

CREATE TABLE known_service_accounts (
    service_account_id uuid PRIMARY KEY,
    name               text NOT NULL
);
