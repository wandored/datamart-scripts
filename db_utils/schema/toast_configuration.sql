-- Configuration API v2 source tables. Run with psql --single-transaction.
-- Existing installations: apply toast_configuration_migration.sql first in the same transaction.
CREATE SCHEMA IF NOT EXISTS toast;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'toast' AND table_name = 'service_charges'
          AND column_name = 'order_id'
    ) THEN
        RAISE EXCEPTION 'Apply toast_configuration_migration.sql before configuration setup';
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS toast.alternate_payment_types (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.break_types (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    active BOOLEAN,
    paid BOOLEAN,
    duration BIGINT,
    enforce_minimum_time BOOLEAN,
    track_missed_breaks BOOLEAN,
    break_interval_hrs BIGINT,
    break_interval_mins BIGINT,
    track_break_acknowledgement BOOLEAN,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.cash_drawers (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    printer JSONB,
    printer_id UUID,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.dining_options (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    behavior TEXT,
    curbside BOOLEAN,
    drive_thru BOOLEAN,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.discounts (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    active BOOLEAN,
    type TEXT,
    percentage NUMERIC,
    amount NUMERIC,
    selection_type TEXT,
    non_exclusive BOOLEAN,
    item_picking_priority TEXT,
    fixed_total NUMERIC,
    promo_codes JSONB,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.menu_groups (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    orderable_online TEXT,
    visibility TEXT,
    unit_of_measure TEXT,
    images JSONB,
    option_groups JSONB,
    inherit_option_groups BOOLEAN,
    inherit_unit_of_measure BOOLEAN,
    menu JSONB,
    menu_id UUID,
    parent JSONB,
    parent_id UUID,
    items JSONB,
    subgroups JSONB,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.menu_items (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    orderable_online TEXT,
    visibility TEXT,
    unit_of_measure TEXT,
    images JSONB,
    option_groups JSONB,
    inherit_option_groups BOOLEAN,
    inherit_unit_of_measure BOOLEAN,
    calories BIGINT,
    sku TEXT,
    plu TEXT,
    type TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.menu_option_groups (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    options JSONB,
    min_selections NUMERIC,
    max_selections NUMERIC,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.menus (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    orderable_online TEXT,
    visibility TEXT,
    unit_of_measure TEXT,
    images JSONB,
    groups JSONB,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.no_sale_reasons (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.payout_reasons (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.pre_modifier_groups (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    is_default BOOLEAN,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.pre_modifiers (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    scale_price BOOLEAN,
    base_price NUMERIC,
    scale_factor NUMERIC,
    display_mode TEXT,
    parent JSONB,
    parent_id UUID,
    charge_as_extra BOOLEAN,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.price_groups (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.printers (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.restaurant_services (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.revenue_centers (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    description TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.sales_categories (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.service_areas (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    service_area_type TEXT,
    revenue_center JSONB,
    revenue_center_id UUID,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.service_charges (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    amount_type TEXT,
    amount NUMERIC,
    percent NUMERIC,
    criteria JSONB,
    gratuity BOOLEAN,
    taxable BOOLEAN,
    applicable_taxes JSONB,
    service_charge_calculation TEXT,
    destination TEXT,
    tax_inclusive BOOLEAN,
    tax_prorated BOOLEAN,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.tables (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    service_area JSONB,
    service_area_id UUID,
    revenue_center JSONB,
    revenue_center_id UUID,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.tax_rates (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    is_default BOOLEAN,
    rate NUMERIC,
    type TEXT,
    rounding_type TEXT,
    tax_table JSONB,
    conditional_tax_rates JSONB,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);

CREATE TABLE IF NOT EXISTS toast.tip_withholding (
    restaurant_id UUID NOT NULL,
    id UUID,
    entity_type TEXT,
    external_id TEXT,
    enabled BOOLEAN,
    percentage NUMERIC,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id)
);

CREATE TABLE IF NOT EXISTS toast.void_reasons (
    restaurant_id UUID NOT NULL,
    id UUID NOT NULL,
    entity_type TEXT,
    external_id TEXT,
    name TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (restaurant_id, id)
);
