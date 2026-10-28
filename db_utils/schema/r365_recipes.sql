-- Apply once before running the weekly CSV imports. No existing tables are replaced.
CREATE SCHEMA IF NOT EXISTS r365;

CREATE TABLE IF NOT EXISTS r365.recipe_items (
    id uuid NOT NULL,
    name text,
    avg_cost numeric,
    reporting_uofm text,
    min_cost numeric,
    max_cost numeric,
    yield_uofm text,
    yield_qty numeric,
    inventory_uofm text,
    inventory_uofm2 text,
    inventory_uofm3 text,
    category_1 text,
    category_2 text,
    category_3 text,
    cost_account text,
    inventory_account text,
    waste_account text,
    key_item boolean,
    number text,
    description text,
    cost_update_method text,
    active boolean,
    available_in_inventory boolean,
    weight_qty numeric,
    weight_uofm text,
    volume_qty numeric,
    volume_uofm text,
    each_qty numeric,
    each_uofm text,
    measure_type text,
    created_by text,
    created_on text,
    modified_by text,
    modified_on text,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS r365.ingredients (
    recipe text NOT NULL,
    item text NOT NULL,
    qty numeric,
    uofm text NOT NULL,
    yield_percent numeric,
    instructions text,
    created_by text,
    created_on text,
    modified_by text,
    modified_on text,
    PRIMARY KEY (recipe, item, uofm)
);
