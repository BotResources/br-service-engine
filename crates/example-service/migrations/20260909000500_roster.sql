CREATE TABLE known_persons (
    user_id      uuid PRIMARY KEY,
    email        text NOT NULL,
    display_name text NOT NULL
);
