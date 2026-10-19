-- Apply once before importing POS and menu item CSVs. Existing tables are retained.
BEGIN;
CREATE SCHEMA IF NOT EXISTS r365;

CREATE TABLE IF NOT EXISTS r365.pos_items (
    id uuid PRIMARY KEY,
    pos_item_id bigint NOT NULL UNIQUE,
    pos_id text,
    sku text,
    barcode text,
    pos_item_name text,
    plu text,
    type text,
    menu_item text,
    created_on text,
    modified_on text,
    is_menu_item boolean,
    modified_by text,
    created_by text
);

CREATE TABLE IF NOT EXISTS r365.menu_items (
    id uuid PRIMARY KEY,
    recipe_id uuid,
    is_recipe_assigned boolean,
    name text,
    type text,
    recipe text,
    category_1 text,
    category_2 text,
    category_3 text,
    created_on text,
    modified_on text
);

COMMENT ON COLUMN r365.pos_items.pos_item_id IS
    'R365 numeric POS item identifier; joins to r365.sales_details.pos_item_id.';
COMMENT ON COLUMN r365.pos_items.menu_item IS
    'Menu item name from CSV, not a menu item UUID. Names are not enforced as unique.';
COMMIT;
