CREATE TABLE IF NOT EXISTS sample_item_pin (
    id uuid PRIMARY KEY,
    item_id uuid NOT NULL REFERENCES sample_lib.item(id)
);
