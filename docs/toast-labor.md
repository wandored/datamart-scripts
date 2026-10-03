# Toast Labor source synchronization

`src/toast_sync/labor.py` is the Labor domain module registered as `--sync labor`.
It uses the existing `ToastClient.request`, `DatabaseConnection`, and bulk writer.
No new authentication system, dependencies, reporting classifications, or KPIs.

Break data is expected to be null and is excluded by request. No breaks table or
JSON column is created. Missing/null breaks do not block updates; any returned
break details are also ignored. Requests do not opt into missed-break details.

## Apply the schema and run

Review/apply [the complete PostgreSQL definitions](../db_utils/schema/toast_labor.sql)
in the target database first. The updater does not execute DDL. The SQL is
transactional and creates missing tables/indexes without dropping existing objects;
`IF NOT EXISTS` does not migrate an incompatible pre-existing table definition.

From the repository root:

```sh
# Validate all four endpoints without database writes (still reads restaurant metadata).
.venv/bin/python -m src.toast-api-update --sync labor --business-date 2026-10-01 --dry-run

# Refresh reference data and actual/scheduled labor for one business day.
.venv/bin/python -m src.toast-api-update --sync labor --business-date 2026-10-01

# Inclusive business-date range; daily requests honor each restaurant's boundaries.
.venv/bin/python -m src.toast-api-update --sync labor --business-date-start 2026-09-28 --business-date-end 2026-10-01

# Reference data only; no date or restaurant timezone metadata required.
.venv/bin/python -m src.toast-api-update --sync labor --labor-resources employees jobs

# Capture edits to old time entries by modification time; end is exclusive.
.venv/bin/python -m src.toast-api-update --sync labor --labor-resources time-entries \
  --labor-modified-start 2026-10-01T00:00:00Z --labor-modified-end 2026-10-02T00:00:00Z

# Modules share the same date options and run in the requested order.
.venv/bin/python -m src.toast-api-update --sync orders labor --business-date 2026-10-01
```

Default `--sync` remains `restaurants`. Selecting Labor defaults to all four
endpoints. `--labor-resources` can restrict the endpoints. Modification filters
apply only to time entries; shifts still require business dates if also selected.
Employees/jobs always request full lists. A single range boundary means one day,
matching the existing Orders behavior. Invalid CLI combinations fail before any
API/database calls. No hard-coded restaurant GUIDs.

## API contract and request strategy

Reviewed against Toast Labor API v1 (specification 1.9.0), 2026-10-02:

| GET endpoint | Supported selection | Implemented behavior |
| --- | --- | --- |
| [/labor/v1/employees](https://doc.toasttab.com/openapi/labor/operation/employeesGet/) | Optional employeeIds, up to 100 | Full restaurant list |
| [/labor/v1/jobs](https://doc.toasttab.com/openapi/labor/operation/jobsGet/) | Optional jobIds, up to 100 | Full list; deleted jobs excluded by Toast |
| [/labor/v1/timeEntries](https://doc.toasttab.com/openapi/labor/operation/timeEntriesGet/) | businessDate, clock-in window, modified window, or timeEntryIds | Clock-in windows with includeArchived=true, or modified windows |
| [/labor/v1/shifts](https://doc.toasttab.com/openapi/labor/operation/shiftsGet/) | Start/end window or shiftIds, up to 100 | Window with look-ahead, filtered locally by scheduled start |

All four GET collection responses are arrays. None documents page/pageSize,
page tokens, or Link pagination. The 100-ID limit is a filter limit, not a
collection page size. The existing Orders paginator is therefore inappropriate.
The shared client supplies restaurant scoping, auth, timeouts, retry handling,
and safe HTTP errors. JSON numbers are decoded directly as `Decimal`.

Time-entry clock-in windows are `[startDate,endDate)`; modified windows are
`[modifiedStartDate,modifiedEndDate)`. Toast's `businessDate` filter excludes
archived entries even with `includeArchived=true`. The updater instead sends
local closeout boundaries through startDate/endDate, with includeArchived=true.
Modified queries include archived entries inherently. Long modified ranges are
split into contiguous 27-day maximum windows, below both the reference's
one-month limit and the developer guide's 30-day limit. Clock-in retrieval uses
one restaurant day per request. No polling watermark is persisted; callers should
rerun an overlapping modification window to recover missed/late edits.

**Shift containment limitation:** Toast requires `inDate >= startDate` AND
`outDate < endDate`. Querying just one business day would omit overnight shifts.
For each requested day the updater queries from its closeout boundary to 27 days
later, then retains only shifts whose scheduled start is inside that day. This
captures normal overnight shifts. A shift ending on/after that look-ahead end,
or with no outDate, may not be returned by Toast. Such unusual schedules need
separate ID-based reconciliation; this implementation cannot promise their
completeness. The API provides no modification-window filter for shifts; refresh
relevant scheduled date ranges again to capture edits. A shift moved entirely
outside refreshed ranges can leave a stale stored record. `Fetched` includes
look-ahead records; `Selected` counts the rows selected for normalization/upsert.
Overlapping look-ahead requests can fetch the same future shift more than once.

## Source model and normalization

The authoritative table definitions are in
[`db_utils/schema/toast_labor.sql`](../db_utils/schema/toast_labor.sql).
All four primary tables use `id UUID PRIMARY KEY`, `restaurant_id UUID NOT NULL`,
`external_id TEXT`, created/modified/deleted `TIMESTAMPTZ`, a nullable deleted flag,
and `last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now()`. No foreign keys.

- [Employee](https://doc.toasttab.com/openapi/labor/tag/Data-definitions/schema/Employee/):
  requested name/contact/external-ID fields, plus phone country code and the
  alternate v2 employee GUID. Names/leading zeros remain unchanged. Only explicit
  whitelisted fields are stored; passcodes and unknown fields are excluded.
  Email and phone are retained for the requested employee matching use case.
- [Job](https://doc.toasttab.com/openapi/labor/tag/Data-definitions/schema/Job/):
  original title/code, default wage, wage frequency, tipped and exclude-from-reporting
  flags. `wage_frequency` distinguishes HOURLY and SALARY. Classification remains
  in core/reporting. Job titles are not keys.
- [TimeEntry](https://doc.toasttab.com/openapi/labor/tag/Data-definitions/schema/TimeEntry/):
  employee/job/shift GUIDs, source business date, in/out timestamps, auto-clock-out,
  regular/overtime hours, historical hourly wage, declared cash tips, non-cash tips,
  tip rounding loss, cash/non-cash gratuity service charges, withheld tips, and
  cash/non-cash sales. No invented regular/overtime pay or adjustment fields.
  Source edits replace stored values on repeat upsert. An open entry may have
  null outDate; hourlyWage may be null for salary jobs.
- [Shift](https://doc.toasttab.com/openapi/labor/tag/Data-definitions/schema/Shift/):
  employee/job IDs, native inDate/outDate mapped to in_date/out_date,
  schedule configuration GUID and four grace periods in minutes. The API has no
  business date, status, or scheduled hours. None is invented. The operational
  index uses `(restaurant_id,in_date)` instead of business_date.

`normalize_employee`, `normalize_job`, `normalize_time_entry`, and `normalize_shift`
are individually testable. Missing optional scalar values remain NULL, including
numerics. All wages, hours, tips, sales, and grace-period numbers use unrestricted
NUMERIC/Decimal; finite numbers and scalar types are validated before writing.
`normalize_batch` rejects an entire malformed endpoint response, reports safe
Toast IDs/field paths, and writes none of that response. Identical repeated IDs
are deduplicated; conflicting duplicates reject the response. Logs never include
raw employee records, passcodes, response bodies, or database exception details
that might contain employee data.

### Employee assignments and wages

Current assigned jobs include jobs never worked, so time entries cannot replace
employee job membership. Current wage overrides also differ from the historical
hourly wage recorded on time entries. Both nested structures merit relations:

- `toast.employee_jobs`: employee_id, job_id, restaurant_id, job_external_id,
  last_synced_at.
- `toast.employee_wage_overrides`: the same fields plus wage NUMERIC.

Neither nested object has an application GUID. Each uses
`PRIMARY KEY (employee_id,job_id)` with the actual Toast employee/job UUIDs.
No fabricated IDs, comma-separated arrays, or relationship JSON. Both children
share the employee refresh timestamp and commit atomically with their employee.
The [Employee model](https://doc.toasttab.com/openapi/labor/tag/Data-definitions/schema/Employee/)
excludes deleted jobs from both arrays. Disappearing assignments/overrides are
retained non-destructively, with their previous last_synced_at. For the most recent
observed employee snapshot, match child.last_synced_at to employee.last_synced_at;
do not assume all retained child rows are current assignments. This is not an
assignment-history archive: re-observed keys update in place.

### Business dates and timezone

Restaurant scope remains `core.restaurants.toast_guid`, matching Restaurants and
Orders. Labor loads timezone/closeout_hour from the corresponding populated
`toast.restaurants` rows in one query. Missing/invalid metadata rejects the
restaurant's dated fact requests without preventing other restaurant/reference
syncs. A modification-only time-entry request needs no local business boundaries.

TimeEntry.businessDate is a yyyyMMdd **string**, converted directly to DATE.
Missing values remain NULL. This differs from Orders' integer businessDate.
Do not infer a missing value from server time, clock-in, or the requested day.

Toast metadata and shift timestamps are documented UTC; examples also use
explicit numeric offsets. All stored API timestamps must have an offset (Z,
+0000, or +00:00 are accepted); naive values fail instead of being assigned the
server timezone. TIMESTAMPTZ preserves the instant. Restaurant ZoneInfo and
closeout_hour are used only for request boundaries, honoring 23/25-hour DST days.
Ambiguous/nonexistent closeout boundaries fail clearly instead of guessing a fold.

The current reference models shiftReference as an ExternalReference object;
[Toast's time-entry guide](https://doc.toasttab.com/doc/devguide/apiGettingTimeEntriesForEmployees.html)
also shows a bare GUID string. Both documented forms are accepted for that field.

## Writes, deletions, and reporting

`upsert_labor_tables` uses the established shared execute_values writer in batches
of 100, with one transaction per restaurant/endpoint/window, including related
employee rows. Primary tables use ON CONFLICT(id); employee relations use their
composite keys. Each refresh updates all source columns and last_synced_at.
A failure rolls back that transaction. Other windows/endpoints/restaurants continue;
any API/normalization/database error produces command exit status 1.

Counts use the existing RETURNING-based inserted/updated mechanism, without
per-row reads. Updates include unchanged source records refreshed by the run.
Counts are accumulated only after commit. The end summary reports each table's
fetched/selected/inserted/updated counts and aggregate errors. A restaurant is
counted processed when at least one requested batch succeeds; aggregate error
counts must also be checked for partial failures. Dry runs fetch/validate but
perform no writes and report zero inserted/updated.

Returned deletion flags/timestamps are preserved. No physical deletion or
inferred deletion occurs for absent records. The unfiltered Jobs list omits
deleted jobs: a previously stored job may retain its former deleted state until
an explicit job-ID refresh is implemented/run separately. Employee/shift full
response completeness regarding deletions should be reconciled with actual Toast
responses; absence never implies deletion here. Historical facts are upserts,
not snapshots or immutable appends. Labor-dollar calculations, scheduled hours,
job categories, and labor percentages belong in reporting.

## Verification before Menus

Automated fixtures do not establish live-account access or source completeness.
Verify against real Toast data:

1. Labor scopes/permissions for all configured restaurants, including archived
   locations; endpoint collection shape and full-list completeness.
2. A known local business day at each timezone/closeout boundary, plus overnight
   shifts and DST where applicable. Compare timeEntry.businessDate directly.
3. Open, edited, and archived entries. Confirm modified-window retrieval finds
   a later edit to an old business date and repeat runs update existing IDs.
4. Salary wages, nullable values, precise tips/hours and source deletion flags;
   reconcile with Toast reports without prematurely calculating pay in source.
5. Employee GUID uniqueness across restaurants, assignments/overrides, and whether
   deleted jobs need a follow-up known-ID retrieval workflow.
6. Both scheduled shift endpoints and API containment behavior; check unusually
   long shifts and rescheduled shifts moved outside the refreshed range.
7. Primary keys/types/indexes/no FKs and multi-table rollback. Reapplying the SQL
   does not validate or change an already incompatible table definition.

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_toast*sync.py'
```

Optional PostgreSQL tests follow the existing convention: a fresh isolated local
cluster, socket `/tmp/toast-restaurants-pg-*`, port 55440, database postgres, user
toast_test, and `TOAST_TEST_PG_SOCKET` pointing at the socket directory. Never point
these fixture-writing tests at production. Labor coverage includes repeat DDL,
UUID/composite upserts, numeric precision, null/deleted edits, refreshed timestamps,
retained missing relationships, atomic rollback, and absence of foreign keys.
