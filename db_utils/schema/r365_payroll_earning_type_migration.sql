-- Existing payroll_summaries tables only. PostgreSQL 15+.
-- Run the entire file in Query Tool with scheduled imports stopped.
-- No rows or columns are removed. Safe to rerun after a successful migration.
BEGIN;

-- A unique index supports the importer's ON CONFLICT column list directly.
-- Its name matches the index created by the current new-install table schema.
CREATE UNIQUE INDEX IF NOT EXISTS payroll_summaries_grain_key
    ON r365.payroll_summaries
    (business_date, location_id, legal_entity_id, employee_id, job_code_id, earning_type, earning_type_name)
    NULLS NOT DISTINCT;

-- Original constraint name, verified against the existing database.
-- Add the expanded key before removing this stricter old key; no CASCADE.
ALTER TABLE r365.payroll_summaries
    DROP CONSTRAINT IF EXISTS payroll_summaries_business_date_location_id_legal_entity_id_key;

COMMIT;

-- Verify: the unique key must include earning_type_name, with no six-column
-- unique key remaining. This returns schema metadata, not payroll records.
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'r365' AND tablename = 'payroll_summaries'
ORDER BY indexname;
