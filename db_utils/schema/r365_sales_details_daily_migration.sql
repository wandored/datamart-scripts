-- One-time migration from ticket-level sales_details to daily item totals.
-- PostgreSQL 15+. Stop scheduled import jobs while applying this transaction.
-- Retains ALL currently reported history, without a date cutoff.
-- Recreates sales_details: ticket line IDs, ticket links and inactive source rows
-- are removed. Dependent views/foreign keys cause failure; NO CASCADE is used.
-- Reapply any custom grants on sales_details to the replacement table.
BEGIN;
LOCK TABLE r365.daily_sales, r365.sales_tickets, r365.sales_details IN ACCESS EXCLUSIVE MODE;

CREATE TABLE r365.sales_account (
    id uuid PRIMARY KEY,
    name text,
    number text,
    gl_type text,
    last_synced_at timestamptz NOT NULL
);

-- Account labels may have changed historically; retain the latest observed label.
INSERT INTO r365.sales_account (id, name, number, gl_type, last_synced_at)
SELECT DISTINCT ON (sales_account_id)
    sales_account_id, sales_account_name, sales_account_number,
    sales_account_gl_type, last_synced_at
FROM r365.sales_details
WHERE sales_account_id IS NOT NULL
ORDER BY sales_account_id, last_synced_at DESC, id DESC;

CREATE TEMP TABLE sales_details_rollup ON COMMIT DROP AS
SELECT summary.business_date, summary.location_id, detail.pos_item_id, detail.pos_item_name,
       detail.void, detail.sales_account_id, detail.menu_item_category_1,
       detail.menu_item_category_2, detail.menu_item_category_3,
       SUM(detail.sale_amount) AS sale_amount, SUM(detail.quantity) AS quantity,
       MAX(detail.last_synced_at) AS last_synced_at
FROM r365.sales_details AS detail
JOIN r365.sales_tickets AS ticket ON ticket.id = detail.sales_ticket_id
JOIN r365.daily_sales AS summary ON summary.id = ticket.daily_sales_id
WHERE detail.is_current AND ticket.is_current
GROUP BY summary.business_date, summary.location_id, detail.pos_item_id, detail.pos_item_name,
         detail.void, detail.sales_account_id, detail.menu_item_category_1,
         detail.menu_item_category_2, detail.menu_item_category_3;

DROP TABLE r365.sales_details;

CREATE TABLE r365.sales_details (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    business_date date NOT NULL,
    location_id uuid NOT NULL,
    pos_item_id bigint,
    pos_item_name text,
    void boolean NOT NULL,
    sales_account_id uuid REFERENCES r365.sales_account (id),
    menu_item_category_1 text,
    menu_item_category_2 text,
    menu_item_category_3 text,
    sale_amount numeric,
    quantity numeric,
    last_synced_at timestamptz NOT NULL,
    is_current boolean NOT NULL DEFAULT true,
    FOREIGN KEY (location_id, business_date)
        REFERENCES r365.daily_sales (location_id, business_date),
    UNIQUE NULLS NOT DISTINCT (
        business_date, location_id, pos_item_id, pos_item_name, void,
        sales_account_id, menu_item_category_1, menu_item_category_2, menu_item_category_3
    )
);
CREATE INDEX sales_details_location_date_idx ON r365.sales_details (location_id, business_date);
CREATE INDEX sales_details_sales_account_idx ON r365.sales_details (sales_account_id);
COMMENT ON TABLE r365.sales_details IS
    'Daily item totals by location, item ID/name, void, sales account and categories; filter is_current.';

INSERT INTO r365.sales_details (
    business_date, location_id, pos_item_id, pos_item_name, void, sales_account_id,
    menu_item_category_1, menu_item_category_2, menu_item_category_3,
    sale_amount, quantity, last_synced_at
)
SELECT business_date, location_id, pos_item_id, pos_item_name, void, sales_account_id,
       menu_item_category_1, menu_item_category_2, menu_item_category_3,
       sale_amount, quantity, last_synced_at
FROM sales_details_rollup;

COMMIT;
