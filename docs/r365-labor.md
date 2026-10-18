# R365 dated labor imports

Apply [r365_labor.sql](../db_utils/schema/r365_labor.sql) once before running the
new endpoints. PostgreSQL 15+ is required for null-safe payroll uniqueness.
For an existing installation, apply
[r365_payroll_earning_type_migration.sql](../db_utils/schema/r365_payroll_earning_type_migration.sql)
with imports stopped. The plain-SQL migration is safe to rerun and ends with
an index-definition query for verification. It expands payroll uniqueness to include the earning
category name, preserves existing rows, and rolls back if a dependent object blocks
the constraint change. Do not rerun the full table setup on existing tables.

The setup creates four new tables in one transaction and fails if they already
exist; it does not replace existing tables. The updater never applies DDL.

```sh
psql --dbname YOUR_DATABASE -v ON_ERROR_STOP=1 -f db_utils/schema/r365_labor.sql
.venv/bin/python -m src.r365-api-update --daily --tables labor_punches payroll_summaries sales_analytics
.venv/bin/python -m src.r365-api-update --week --tables labor_punches payroll_summaries sales_analytics
.venv/bin/python -m src.r365-api-update --bulk -y 2026 -p 10 -w 2 --tables labor_punches payroll_summaries sales_analytics
```

All three are included in the daily, weekly, and bulk defaults. `--week` uses
`core.calendar` to find yesterday's fiscal week and imports each date through
yesterday. Explicit `--bulk --business-date YYYY-MM-DD` also supports future
forecast dates. Bulk fiscal dates likewise come from `core.calendar`.

| Selection | Tables | Daily/week filters | Bulk filters |
| --- | --- | --- | --- |
| `labor_punches` | `r365.labor_punches` | `modifiedOnStart`, `modifiedOnEnd` timestamps | `dateOfBusinessStart`, `dateOfBusinessEnd` |
| `payroll_summaries` | `r365.payroll_summaries` | `businessDateStart`, `businessDateEnd` | Same business-date filters |
| `sales_analytics` | `r365.sales_analytics`, `r365.sales_analytics_jobs` | `startDate`, `endDate` | Same business-date filters |

Payroll and analytics have no documented modification filter. Daily/week
refresh their selected business dates, so older corrections require a bulk rerun.
Locations come from the complete R365 locations endpoint, including unchanged
locations. Payroll and analytics request one location/day at a time; punches
fetch all accessible locations. This scope is independent of the active-restaurant
filter used by daily sales.

## Stored data and consistency

[Punches](https://docs.restaurant365.com/apidocs/public-v1-labor-labor-punches)
use the source `laborId` as the `labor_id` primary key. Hours, rates, amounts,
work dates, timestamps, and employee/job/location/audit-user IDs are retained.
All continuation pages must succeed before writes. Modification responses are
incremental: absence never deletes or deactivates a punch. The endpoint does not
expose a documented deletion indicator, so source-deleted punches may remain.

[Payroll summaries](https://docs.restaurant365.com/apidocs/public-v1-labor-payroll-summaries)
are keyed by business date, location, legal entity, employee, job, earning-type
code, and earning-type name. Live responses can have a null `earningType` for
both Regular Pay and Charge Tips, so `earningTypeName` must distinguish those
rows. This is a pay category, not an employee name. Null job/earning/entity values
participate in a null-safe unique constraint.
Totals overwrite previous values rather than accumulating. The documented
response is a raw array with no continuation metadata, although `pageSize` is
also documented. An exact 250-row location/day response fails before writes,
since it could be truncated; confirm the server's completeness contract before
relaxing this check. Other malformed envelopes also fail.

[Sales analytics](https://docs.restaurant365.com/apidocs/public-v1-labor-sales-analytics)
contains published forecasts and actuals, not sales tickets. Interval rows are
keyed by location, business date, and interval start. Job rows add the source job
UUID to that key and reference the interval. Forecast, actual, optimal-labor,
and variance measures are preserved, along with publication timestamps.
The parser accepts live camelCase fields (`dailyData`, `intervals`, `jobDetails`,
and numeric measures) as well as the PascalCase variants in the documentation.
Conflicting aliases and genuinely missing arrays fail before writes.

For payroll and analytics, a complete valid response marks existing rows for
that location/date inactive before upserting current rows. Explicitly empty
responses therefore deactivate that snapshot. Missing arrays or conflicting
duplicate keys fail validation before writes. Analytics intervals and job rows
commit together; a failure rolls back their updates and inactivation. No rows
are deleted. Filter aggregate reports with `is_current = true`.

References are stored as source UUIDs; redundant names, payroll-provider aliases,
and publisher names are intentionally omitted. These imports do not apply the
employee import's POS-UUID filter and do not depend on reference tables having
been fully loaded. Join reference IDs where available. Numeric measures use
PostgreSQL `numeric`; timestamps use `timestamptz`. Offset-free timestamps require
a recognized location timezone; ambiguous/nonexistent local DST times fail
rather than silently changing the instant.

These schemas and request contracts are covered by synthetic unit and isolated
PostgreSQL tests. Sales-analytics field structure has also been checked with read-only live requests;
production writes have not been run as part of verification.

The corrected payroll key was validated read-only against all 42 accessible
locations for 2026-10-12 (731 rows), without database writes.
