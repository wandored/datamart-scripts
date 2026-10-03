-- One-time setup. Run explicitly before enabling the daily-sales sync.
-- Deliberately fails if any target table already exists; never replaces tables.
BEGIN;
CREATE SCHEMA IF NOT EXISTS r365;

CREATE TABLE r365.daily_sales (
    id uuid PRIMARY KEY,
    location_id uuid NOT NULL,
    business_date date NOT NULL,
    net_sales numeric,
    gross_sales numeric,
    guest_count integer,
    total_labor_hours numeric,
    total_labor_amount numeric,
    total_labor_percentage numeric,
    total_deposit numeric,
    expected_cash numeric,
    paid_in_total numeric,
    paid_out_total numeric,
    over_short numeric,
    last_synced_at timestamptz NOT NULL,
    UNIQUE (location_id, business_date)
);
CREATE INDEX daily_sales_business_date_idx ON r365.daily_sales (business_date);

CREATE TABLE r365.sales_tickets (
    id uuid PRIMARY KEY,
    daily_sales_id uuid NOT NULL REFERENCES r365.daily_sales (id),
    receipt_number text,
    check_number text,
    sale_date_time timestamptz,
    day_of_week text,
    day_part text,
    net_sales numeric,
    gross_sales numeric,
    sales_amount numeric,
    guest_count integer,
    order_hour integer,
    tax_amount numeric,
    tip_amount numeric,
    total_amount numeric,
    total_payment numeric,
    void boolean NOT NULL,
    server_id uuid,
    last_synced_at timestamptz NOT NULL,
    is_current boolean NOT NULL DEFAULT true
);
CREATE INDEX sales_tickets_daily_sales_id_idx ON r365.sales_tickets (daily_sales_id);

CREATE TABLE r365.sales_details (
    id uuid PRIMARY KEY,
    sales_ticket_id uuid NOT NULL REFERENCES r365.sales_tickets (id),
    pos_item_id bigint,
    pos_item_name text,
    sale_amount numeric,
    quantity numeric,
    void boolean NOT NULL,
    sales_account_id uuid,
    sales_account_name text,
    sales_account_number text,
    sales_account_gl_type text,
    sales_category text,
    menu_item_category_1 text,
    menu_item_category_2 text,
    menu_item_category_3 text,
    last_synced_at timestamptz NOT NULL,
    is_current boolean NOT NULL DEFAULT true
);
CREATE INDEX sales_details_sales_ticket_id_idx ON r365.sales_details (sales_ticket_id);

CREATE TABLE r365.sales_payments (
    id uuid PRIMARY KEY,
    sales_ticket_id uuid NOT NULL REFERENCES r365.sales_tickets (id),
    amount numeric,
    payment_type text,
    payment_group text,
    payment_date timestamptz,
    last_synced_at timestamptz NOT NULL,
    is_current boolean NOT NULL DEFAULT true
);
CREATE INDEX sales_payments_sales_ticket_id_idx ON r365.sales_payments (sales_ticket_id);

CREATE TABLE r365.sales_ticket_taxes (
    sales_ticket_id uuid NOT NULL REFERENCES r365.sales_tickets (id),
    tax_index integer NOT NULL CHECK (tax_index >= 0),
    rate_id text,
    name text,
    rate numeric,
    tax_type text,
    inclusion_type text,
    applied_tax_portion_amount numeric,
    last_synced_at timestamptz NOT NULL,
    is_current boolean NOT NULL DEFAULT true,
    PRIMARY KEY (sales_ticket_id, tax_index)
);
-- The composite primary key also indexes sales_ticket_id.
COMMENT ON COLUMN r365.sales_ticket_taxes.tax_index IS
    'Zero-based position in the current ticket tax array; not a permanent source tax ID.';
COMMENT ON COLUMN r365.sales_tickets.server_id IS
    'R365 server reference only; server names, payroll IDs and ticket comments are not imported.';
COMMIT;
