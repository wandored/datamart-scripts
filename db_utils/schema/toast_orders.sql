-- Apply explicitly before enabling --sync orders. No foreign keys or replacement.
BEGIN;
CREATE SCHEMA IF NOT EXISTS toast;
CREATE TABLE IF NOT EXISTS toast.orders (
    id UUID NOT NULL,
    restaurant_id UUID NOT NULL,
    business_date DATE NOT NULL,
    external_id TEXT,
    display_number TEXT,
    source TEXT,
    approval_status TEXT,
    created_by_client_name TEXT,
    required_prep_time TEXT,
    number_of_guests INTEGER,
    calculated_guest_count NUMERIC,
    duration INTEGER,
    opened_date TIMESTAMPTZ,
    modified_date TIMESTAMPTZ,
    promised_date TIMESTAMPTZ,
    created_date TIMESTAMPTZ,
    paid_date TIMESTAMPTZ,
    closed_date TIMESTAMPTZ,
    deleted_date TIMESTAMPTZ,
    void_date TIMESTAMPTZ,
    estimated_fulfillment_date TIMESTAMPTZ,
    void_business_date DATE,
    voided BOOLEAN,
    deleted BOOLEAN,
    created_in_test_mode BOOLEAN,
    excess_food BOOLEAN,
    server_id UUID,
    dining_option_id UUID,
    table_id UUID,
    service_area_id UUID,
    restaurant_service_id UUID,
    revenue_center_id UUID,
    channel_id UUID,
    created_device_id TEXT,
    last_modified_device_id TEXT,
    pricing_features TEXT[],
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT orders_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS orders_restaurant_business_date_idx
    ON toast.orders (restaurant_id, business_date);
-- Additive upgrade for the existing order-header table. Existing source types
-- and columns are retained. Re-fetch history to give old rows an observation time.
ALTER TABLE toast.orders
    ADD COLUMN IF NOT EXISTS last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS orders_business_date_idx ON toast.orders (business_date);
CREATE INDEX IF NOT EXISTS orders_modified_date_idx ON toast.orders (modified_date);
-- The existing (restaurant_id, business_date) index also supports restaurant filters.

CREATE TABLE IF NOT EXISTS toast.checks (
    id UUID NOT NULL,
    order_id UUID NOT NULL,
    restaurant_id UUID NOT NULL,
    business_date DATE,
    display_number TEXT,
    tab_name TEXT,
    payment_status TEXT,
    created_date TIMESTAMPTZ,
    opened_date TIMESTAMPTZ,
    closed_date TIMESTAMPTZ,
    modified_date TIMESTAMPTZ,
    deleted_date TIMESTAMPTZ,
    paid_date TIMESTAMPTZ,
    void_date TIMESTAMPTZ,
    amount NUMERIC,
    tax_amount NUMERIC,
    total_amount NUMERIC,
    tax_exempt BOOLEAN,
    voided BOOLEAN,
    deleted BOOLEAN,
    void_business_date DATE,
    duration INTEGER,
    opened_by_id UUID,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT checks_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS checks_order_id_idx ON toast.checks (order_id);

CREATE TABLE IF NOT EXISTS toast.selections (
    id UUID NOT NULL,
    order_id UUID NOT NULL,
    restaurant_id UUID NOT NULL,
    business_date DATE,
    check_id UUID NOT NULL,
    parent_selection_id UUID,
    item_id UUID,
    item_group_id UUID,
    option_group_id UUID,
    pre_modifier_id UUID,
    sales_category_id UUID,
    dining_option_id UUID,
    void_reason_id UUID,
    refund_transaction_id UUID,
    display_name TEXT,
    plu TEXT,
    pre_modifier_plu TEXT,
    sales_category_plu TEXT,
    unit_of_measure TEXT,
    selection_type TEXT,
    fulfillment_status TEXT,
    tax_inclusion TEXT,
    option_group_pricing_mode TEXT,
    quantity NUMERIC,
    pre_discount_price NUMERIC,
    price NUMERIC,
    receipt_line_price NUMERIC,
    open_price_amount NUMERIC,
    external_price_amount NUMERIC,
    tax NUMERIC,
    guest_count_weight NUMERIC,
    refund_amount NUMERIC,
    tax_refund_amount NUMERIC,
    seat_number INTEGER,
    voided BOOLEAN,
    deferred BOOLEAN,
    void_date TIMESTAMPTZ,
    created_date TIMESTAMPTZ,
    modified_date TIMESTAMPTZ,
    void_business_date DATE,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT selections_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS selections_order_id_idx ON toast.selections (order_id);
CREATE INDEX IF NOT EXISTS selections_check_id_idx ON toast.selections (check_id);
CREATE INDEX IF NOT EXISTS selections_parent_selection_id_idx ON toast.selections (parent_selection_id);

CREATE TABLE IF NOT EXISTS toast.payments (
    id UUID NOT NULL,
    order_id UUID NOT NULL,
    restaurant_id UUID NOT NULL,
    business_date DATE,
    check_id UUID NOT NULL,
    paid_date TIMESTAMPTZ,
    refund_date TIMESTAMPTZ,
    void_date TIMESTAMPTZ,
    paid_business_date DATE,
    refund_business_date DATE,
    void_business_date DATE,
    type TEXT,
    card_entry_mode TEXT,
    card_type TEXT,
    refund_status TEXT,
    payment_status TEXT,
    amount NUMERIC,
    tip_amount NUMERIC,
    amount_tendered NUMERIC,
    refund_amount NUMERIC,
    tip_refund_amount NUMERIC,
    server_id UUID,
    cash_drawer_id UUID,
    house_account_id UUID,
    other_payment_id UUID,
    refund_transaction_id UUID,
    void_reason_id UUID,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT payments_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS payments_order_id_idx ON toast.payments (order_id);
CREATE INDEX IF NOT EXISTS payments_check_id_idx ON toast.payments (check_id);

CREATE TABLE IF NOT EXISTS toast.applied_discounts (
    id UUID NOT NULL,
    order_id UUID NOT NULL,
    restaurant_id UUID NOT NULL,
    business_date DATE,
    check_id UUID NOT NULL,
    selection_id UUID,
    discount_id UUID,
    approver_id UUID,
    discount_reason_id UUID,
    name TEXT,
    discount_type TEXT,
    discount_plu TEXT,
    processing_state TEXT,
    applied_promo_code TEXT,
    reason_name TEXT,
    reason_description TEXT,
    reason_comment TEXT,
    discount_amount NUMERIC,
    non_tax_discount_amount NUMERIC,
    discount_percent NUMERIC,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT applied_discounts_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS applied_discounts_order_id_idx ON toast.applied_discounts (order_id);
CREATE INDEX IF NOT EXISTS applied_discounts_check_id_idx ON toast.applied_discounts (check_id);

CREATE TABLE IF NOT EXISTS toast.service_charges (
    id UUID NOT NULL,
    order_id UUID NOT NULL,
    restaurant_id UUID NOT NULL,
    business_date DATE,
    check_id UUID NOT NULL,
    service_charge_id UUID,
    payment_id UUID,
    refund_transaction_id UUID,
    name TEXT,
    charge_type TEXT,
    service_charge_calculation TEXT,
    service_charge_category TEXT,
    charge_amount NUMERIC,
    refund_amount NUMERIC,
    tax_refund_amount NUMERIC,
    delivery BOOLEAN,
    takeout BOOLEAN,
    dine_in BOOLEAN,
    gratuity BOOLEAN,
    taxable BOOLEAN,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT service_charges_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS service_charges_order_id_idx ON toast.service_charges (order_id);
CREATE INDEX IF NOT EXISTS service_charges_check_id_idx ON toast.service_charges (check_id);

CREATE TABLE IF NOT EXISTS toast.applied_taxes (
    id UUID NOT NULL,
    order_id UUID NOT NULL,
    restaurant_id UUID NOT NULL,
    business_date DATE,
    check_id UUID NOT NULL,
    selection_id UUID,
    service_charge_id UUID,
    tax_inclusion TEXT,
    tax_rate_id UUID,
    name TEXT,
    display_name TEXT,
    type TEXT,
    jurisdiction TEXT,
    jurisdiction_type TEXT,
    rate NUMERIC,
    tax_amount NUMERIC,
    facilitator_collect_and_remit_tax BOOLEAN,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT applied_taxes_pkey PRIMARY KEY (id),
    CONSTRAINT applied_taxes_one_parent CHECK ((selection_id IS NOT NULL) <> (service_charge_id IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS applied_taxes_order_id_idx ON toast.applied_taxes (order_id);
CREATE INDEX IF NOT EXISTS applied_taxes_check_id_idx ON toast.applied_taxes (check_id);
CREATE INDEX IF NOT EXISTS applied_taxes_selection_id_idx ON toast.applied_taxes (selection_id);
CREATE INDEX IF NOT EXISTS applied_taxes_service_charge_id_idx ON toast.applied_taxes (service_charge_id);

COMMIT;
