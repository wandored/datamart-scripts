# Toast source updater

Available resources: `restaurants`, `orders`, `labor`, and `configuration`. For order setup, date arguments,
field mapping, and verification, see [toast-orders.md](toast-orders.md).

Run from the repository root using the existing environment and configuration.
Apply `db_utils/schema/toast_restaurants.sql` explicitly to the intended database
first, for example with an already configured `psql` connection:

```sh
psql -X -v ON_ERROR_STOP=1 -f db_utils/schema/toast_restaurants.sql
.venv/bin/python -m src.toast-api-update --dry-run
.venv/bin/python -m src.toast-api-update --sync restaurants
```

Without a mode, omitting `--sync` syncs restaurants. `--sync` accepts one or more
registered resource names; duplicates run once, in the requested order. Currently
`restaurants`, `orders`, `labor`, and `configuration` are registered. The restaurant setup SQL creates only
the schema and requested table if missing, with UUID identifiers, a primary key,
and no foreign keys. It does not migrate an incompatible existing table. The
updater itself performs no DDL. Dry run reads company configuration and calls the
API, including the existing authentication/cache mechanism, but writes no database rows.

## Daily, weekly, and fiscal period runs

All three modes update every registered resource by default, including all Labor
endpoints. Use `--sync` (or `--tables`) to select resources, and
`--labor-resources` to narrow Labor endpoints, and `--config-resources` to narrow
configuration endpoints. `--sync all` explicitly selects all.

```sh
.venv/bin/python -m src.toast-api-update --daily
.venv/bin/python -m src.toast-api-update --daily 2026-10-08 --sync orders labor
.venv/bin/python -m src.toast-api-update --weekly --sync orders labor
.venv/bin/python -m src.toast-api-update --period 9 10 --year 2026
.venv/bin/python -m src.toast-api-update --period 9 --year 2026 --week 5 --sync orders
.venv/bin/python -m src.toast-api-update --daily --sync labor --labor-resources time-entries --dry-run
```

`--daily` selects yesterday only; `--daily YYYY-MM-DD` selects that date. Daily
mode does not query `core.calendar`. `--weekly` selects
the complete fiscal week before today's fiscal week. `--period` requires a fiscal
`--year` and accepts one or more periods; optional `--week` requires a single
period. Both updaters share the same `core.calendar` resolution, including fiscal
year boundaries and fifth weeks. Period gaps remain separate ranges.

Orders, time entries, and shifts use those dates through the existing Toast
business-date/time-zone handling. Restaurants, configuration, employees, and jobs
refresh once per invocation. Configuration uses its own optional
`--config-last-modified` timestamp; mode dates do not set this filter. Without
the filter, configuration performs a full fetch. All required source schemas must already exist. Modes cannot
be combined with explicit business dates or Labor modification timestamps;
existing commands using those arguments without a mode remain supported.

## Source and mapping

The new updater uses all non-null `core.restaurants.toast_guid` values. This is
the recommended canonical source, already used by product mix. It does not filter
on email or active status, so inactive/archived source records can still update.
Legacy email-based selection and management-group discovery/exclusions remain
available to existing callers; those approaches have not been consolidated.

The [Restaurants API](https://doc.toasttab.com/openapi/restaurants/operation/restaurantsRestaurantGuidGet/)
returns a single object. Requests include `includeArchived=true`.

| Database column | API field |
| --- | --- |
| id | guid |
| name | general.name |
| location_name | general.locationName |
| location_code | general.locationCode |
| timezone | general.timeZone |
| closeout_hour | general.closeoutHour |
| management_group_id | general.managementGroupGuid |
| currency_code | general.currencyCode |
| first_business_date | general.firstBusinessDate (integer yyyyMMdd to DATE) |
| archived | general.archived |

Missing/null optional fields become SQL NULL. Supplied malformed values, missing
GUID/general objects, and a GUID differing from the requested restaurant reject
the row. No company names, time zones, or other master values overwrite Toast data.

## Behavior and reusable entry points

- `ToastClient.request`: authenticated restaurant-scoped GET, 60-second timeout,
  up to three attempts for connection/timeouts and HTTP 429/500/502/503/504.
  Numeric Retry-After values up to 60 seconds are honored; longer or unsupported
  values fail for a later run. Authentication/permission errors are not retried.
- `ToastClient.get_restaurant`: returns the complete RestaurantInfo object.
- Existing `get_response_data` and `get_paged_response_data` retain token and
  page-number pagination respectively and now use `request`. Failed pages raise
  instead of returning partial data. Existing successful response shapes remain unchanged.
- Token cache loading now supports the authentication response's `accessToken`
  object as well as legacy strings. `load_locations=False` avoids the legacy
  implicit database lookup when the updater has selected its own locations.
- `src/toast_sync/locations.py` provides shared `get_configured_restaurants`.
- `src/toast_sync/restaurants.py` owns `normalize_restaurant`, `upsert_restaurants`,
  and `sync_restaurants`. Each future API resource gets its own module in this package.
- `src/toast_sync/orders.py` owns normalization of orders and their checks,
  selections, payments, discounts, and taxes, plus date validation
  and per-restaurant/business-date synchronization.
- `src/toast_sync/configuration.py` owns all 24 configuration endpoints.
  See [Configuration setup, migration, and incremental updates](toast-configuration.md).
- `src/toast_sync/labor.py` owns Labor endpoints and their normalized tables.
  See [Labor setup, schema, and examples](toast-labor.md).
- `src/toast_sync/dates.py` registers the shared business-date CLI arguments.
- `src/toast_sync/values.py` shares strict source scalar parsing between Orders and Labor.
- `src/toast_sync/writing.py` shares the transactional UUID bulk-upsert implementation.
- `src/toast-api-update.py` handles CLI arguments, initializes a shared client and
  location list, and dispatches the selected modules. API helpers contain no new
  table-write logic.

To add a resource, create its module, import it into
`toast-api-update.py`, and add a name/module entry to `SYNC_MODULES`. Removing
that entry removes the CLI choice. Each module exposes `sync`, accepting
`(client, locations, dry_run=False, **resource_options)` and returning counts with
`api_errors` and `rejected`. Modules may also return `database_errors`, or raise.
Any nonzero error count makes the command fail. Optional `add_arguments(parser)`
and `get_options(args)` hooks let modules register and validate their own CLI
options before external calls. `get_options` returns keyword arguments for `sync`
and raises `ValueError` for invalid options. Keep resource-specific
normalization and writes in that module and reusable HTTP/authentication in
`ToastClient`. CLI choices come directly from the registry. Adding a resource
adds it to the default mode runs. Without a mode, the default remains restaurants only.

Selected modules run sequentially. Restaurants use one transaction; orders use
  one transaction per restaurant/business date. Labor uses one transaction per
restaurant/endpoint/window, including its employee relationships. A failure in one
module does not prevent later selected modules from running, but makes the
overall exit status 1. Successful modules are not rolled back because another fails.

The writer reuses `DatabaseConnection.executemany`/`execute_values` in batches of
100 and commits all valid restaurant rows together. The existing generic upsert
function is local to the R365 script, rather than a shared database helper; it
does not return inserted/updated counts and is left untouched.
Counts use `RETURNING (xmax = 0)` for each upserted row; updated includes unchanged
rows that took the conflict-update branch. Logs report counts after commit only.
Database errors abort the batch. API/validation errors identify the company ID
and Toast GUID, leave that restaurant's stored row untouched, and allow other
valid restaurants to update. Any failure gives exit status 1, including partial
success; full success or no configured restaurants gives 0. No deletes occur.

## Restaurant verification

Verify API access to every configured restaurant, including archived locations;
review nulls and business-date values against actual responses. Confirm the
target table has the requested types and primary key, and no foreign keys.
Run twice and confirm the second run updates existing IDs without increasing row
count. Check a changed source name/status updates and an API failure preserves
the previously stored row. Confirm `toast.restaurants.id` logically joins to
`core.restaurants.toast_guid`. Live API/production behavior must be verified in
your environment; automated tests use synthetic responses and an isolated database.

Safe unit verification:

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_toast_restaurants_sync.py'
```

The optional database test requires a fresh local PostgreSQL instance with Unix
socket directory `/tmp/toast-restaurants-pg-*`, port 55440, database `postgres`,
and user `toast_test`. Set `TOAST_TEST_PG_SOCKET` to that directory to enable it.
It checks repeat updates, multiple bulk pages, NULLs, rollback, and no foreign keys.
The Orders module writes six Orders-derived source tables. Labor is available
with `--sync labor`; configuration uses `--sync configuration`. The configuration
migration retires the old Orders-derived service_charges table. Kitchen endpoints
remain a future module.
