-- One-time setup for new tables; PostgreSQL 15+ is required.
-- Run explicitly. Existing tables cause an error and rollback, not replacement.
BEGIN;
CREATE SCHEMA IF NOT EXISTS r365;

CREATE TABLE r365.labor_punches (
    labor_id uuid PRIMARY KEY,
    date_worked date NOT NULL,
    start_time timestamptz,
    end_time timestamptz,
    hours numeric,
    pay_rate numeric,
    total numeric,
    tip_declared_amount numeric,
    employee_id uuid,
    job_id uuid,
    location_id uuid,
    created_on timestamptz,
    created_by_id uuid,
    modified_on timestamptz,
    modified_by_id uuid,
    last_synced_at timestamptz NOT NULL
);
CREATE INDEX labor_punches_location_date_idx ON r365.labor_punches (location_id, date_worked);
CREATE INDEX labor_punches_employee_idx ON r365.labor_punches (employee_id);

CREATE TABLE r365.payroll_summaries (
    business_date date NOT NULL,
    location_id uuid NOT NULL,
    legal_entity_id uuid,
    employee_id uuid NOT NULL,
    job_code_id uuid,
    earning_type text,
    earning_type_name text,
    total_seconds bigint,
    rate numeric,
    cost numeric,
    last_synced_at timestamptz NOT NULL,
    is_current boolean NOT NULL DEFAULT true,
    CONSTRAINT payroll_summaries_grain_key UNIQUE NULLS NOT DISTINCT
        (business_date, location_id, legal_entity_id, employee_id, job_code_id, earning_type, earning_type_name)
);
CREATE INDEX payroll_summaries_location_date_idx ON r365.payroll_summaries (location_id, business_date);

CREATE TABLE r365.sales_analytics (
    location_id uuid NOT NULL,
    business_date date NOT NULL,
    start_date_time timestamptz NOT NULL,
    published_at timestamptz,
    forecasted_net_sales numeric,
    forecasted_gross_sales numeric,
    forecasted_entree numeric,
    forecasted_labor numeric,
    actual_net_sales numeric,
    actual_gross_sales numeric,
    actual_entree_count numeric,
    actual_labor numeric,
    actual_labor_ot_hours numeric,
    actual_labor_dollars numeric,
    optimal_labor_variance numeric,
    optimal_labor numeric,
    last_synced_at timestamptz NOT NULL,
    is_current boolean NOT NULL DEFAULT true,
    PRIMARY KEY (location_id, business_date, start_date_time)
);
CREATE TABLE r365.sales_analytics_jobs (
    location_id uuid NOT NULL,
    business_date date NOT NULL,
    start_date_time timestamptz NOT NULL,
    job_id uuid NOT NULL,
    labor_forecast numeric,
    actual_labor numeric,
    actual_labor_ot_hours numeric,
    actual_labor_dollars numeric,
    last_synced_at timestamptz NOT NULL,
    is_current boolean NOT NULL DEFAULT true,
    PRIMARY KEY (location_id, business_date, start_date_time, job_id),
    FOREIGN KEY (location_id, business_date, start_date_time)
        REFERENCES r365.sales_analytics (location_id, business_date, start_date_time)
);
COMMIT;
