-- Apply once before running python -m src.menu-item-map --write.
BEGIN;
CREATE TABLE IF NOT EXISTS core.menu_item_mapping (
    restaurant_id integer NOT NULL REFERENCES core.restaurants(id),
    toast_menu_item_id uuid NOT NULL,
    r365_menu_item_id uuid NOT NULL REFERENCES r365.menu_items(id),
    r365_pos_item_id bigint NOT NULL REFERENCES r365.pos_items(pos_item_id),
    match_method text NOT NULL CHECK (match_method IN ('concept_name', 'manual', 'api')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, toast_menu_item_id)
);
CREATE INDEX IF NOT EXISTS menu_item_mapping_r365_menu_idx
    ON core.menu_item_mapping (r365_menu_item_id);
CREATE INDEX IF NOT EXISTS menu_item_mapping_r365_pos_idx
    ON core.menu_item_mapping (restaurant_id, r365_pos_item_id);
COMMENT ON TABLE core.menu_item_mapping IS
    'Restaurant-scoped Toast items mapped to concept-level R365 menu and POS IDs. Existing decisions are never overwritten by automatic matching.';
COMMIT;
