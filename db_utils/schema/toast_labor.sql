-- Toast Labor API v1. Source values only; no foreign keys or destructive refreshes.
BEGIN;
CREATE SCHEMA IF NOT EXISTS toast;

CREATE TABLE IF NOT EXISTS toast.employees (
    id UUID NOT NULL PRIMARY KEY,
    restaurant_id UUID NOT NULL,
    external_id TEXT,
    external_employee_id TEXT,
    first_name TEXT,
    chosen_name TEXT,
    last_name TEXT,
    email TEXT,
    phone_number TEXT,
    phone_number_country_code TEXT,
    created_date TIMESTAMPTZ,
    modified_date TIMESTAMPTZ,
    deleted_date TIMESTAMPTZ,
    deleted BOOLEAN,
    v2_employee_guid UUID,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS toast.jobs (
    id UUID NOT NULL PRIMARY KEY,
    restaurant_id UUID NOT NULL,
    external_id TEXT,
    title TEXT,
    code TEXT,
    default_wage NUMERIC,
    wage_frequency TEXT,
    tipped BOOLEAN,
    exclude_from_reporting BOOLEAN,
    created_date TIMESTAMPTZ,
    modified_date TIMESTAMPTZ,
    deleted_date TIMESTAMPTZ,
    deleted BOOLEAN,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS toast.employee_jobs (
    employee_id UUID NOT NULL,
    job_id UUID NOT NULL,
    restaurant_id UUID NOT NULL,
    job_external_id TEXT,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (employee_id, job_id)
);

CREATE TABLE IF NOT EXISTS toast.employee_wage_overrides (
    employee_id UUID NOT NULL,
    job_id UUID NOT NULL,
    restaurant_id UUID NOT NULL,
    job_external_id TEXT,
    wage NUMERIC,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (employee_id, job_id)
);

CREATE TABLE IF NOT EXISTS toast.time_entries (
    id UUID NOT NULL PRIMARY KEY,
    restaurant_id UUID NOT NULL,
    external_id TEXT,
    employee_id UUID,
    job_id UUID,
    shift_id UUID,
    business_date DATE,
    in_date TIMESTAMPTZ,
    out_date TIMESTAMPTZ,
    auto_clocked_out BOOLEAN,
    regular_hours NUMERIC,
    overtime_hours NUMERIC,
    hourly_wage NUMERIC,
    declared_cash_tips NUMERIC,
    non_cash_tips NUMERIC,
    non_cash_tips_rounding_loss NUMERIC,
    cash_gratuity_service_charges NUMERIC,
    non_cash_gratuity_service_charges NUMERIC,
    tips_withheld NUMERIC,
    cash_sales NUMERIC,
    non_cash_sales NUMERIC,
    created_date TIMESTAMPTZ,
    modified_date TIMESTAMPTZ,
    deleted_date TIMESTAMPTZ,
    deleted BOOLEAN,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS toast.shifts (
    id UUID NOT NULL PRIMARY KEY,
    restaurant_id UUID NOT NULL,
    external_id TEXT,
    employee_id UUID,
    job_id UUID,
    in_date TIMESTAMPTZ,
    out_date TIMESTAMPTZ,
    schedule_config_id UUID,
    min_before_clock_in NUMERIC,
    min_after_clock_in NUMERIC,
    min_before_clock_out NUMERIC,
    min_after_clock_out NUMERIC,
    created_date TIMESTAMPTZ,
    modified_date TIMESTAMPTZ,
    deleted_date TIMESTAMPTZ,
    deleted BOOLEAN,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS employees_restaurant_id_idx ON toast.employees (restaurant_id);
CREATE INDEX IF NOT EXISTS jobs_restaurant_id_idx ON toast.jobs (restaurant_id);
CREATE INDEX IF NOT EXISTS employee_jobs_restaurant_id_idx ON toast.employee_jobs (restaurant_id);
CREATE INDEX IF NOT EXISTS employee_wage_overrides_restaurant_id_idx ON toast.employee_wage_overrides (restaurant_id);
CREATE INDEX IF NOT EXISTS time_entries_employee_id_idx ON toast.time_entries (employee_id);
CREATE INDEX IF NOT EXISTS time_entries_job_id_idx ON toast.time_entries (job_id);
CREATE INDEX IF NOT EXISTS time_entries_business_date_idx ON toast.time_entries (business_date);
CREATE INDEX IF NOT EXISTS time_entries_restaurant_business_date_idx ON toast.time_entries (restaurant_id, business_date);
CREATE INDEX IF NOT EXISTS time_entries_modified_date_idx ON toast.time_entries (modified_date);
CREATE INDEX IF NOT EXISTS shifts_employee_id_idx ON toast.shifts (employee_id);
CREATE INDEX IF NOT EXISTS shifts_job_id_idx ON toast.shifts (job_id);
CREATE INDEX IF NOT EXISTS shifts_restaurant_in_date_idx ON toast.shifts (restaurant_id, in_date);
COMMIT;
