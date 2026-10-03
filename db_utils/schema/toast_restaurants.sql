-- Apply explicitly before running the updater. Existing tables are never replaced.
BEGIN;
CREATE SCHEMA IF NOT EXISTS toast;

CREATE TABLE IF NOT EXISTS toast.restaurants
(
    id                  UUID NOT NULL,
    name                TEXT,
    location_name       TEXT,
    location_code       TEXT,
    timezone            TEXT,
    closeout_hour       INTEGER,
    management_group_id UUID,
    currency_code       TEXT,
    first_business_date DATE,
    archived            BOOLEAN,

    CONSTRAINT restaurants_pkey PRIMARY KEY (id)
);
COMMIT;
