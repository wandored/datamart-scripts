# DataMart Scripts
Collection of utilities and scripts used to manage and update DataMart tables

## File downloads:
### Restaurant365 Files:
* MenuItems_R365.csv
* Menu Price Analysis.csv
* Product Mix.csv
* RecipeItems.csv
* ingredients.csv
* Receiving by Purchased Item.csv (item specific)

### Toast Files:
* MenuItem_Export_toast.csv

### Company Files:
* budgets/*.csv (store budgets)
* fiscal_calendar_2024.csv

## Utility Files:
* config.py # configuration settings
* dbconnect.py # database connection utility
* recreate_views.py # recreates views in datamart
* specialty.txt # list of specialty items to exclude from menu engineering

## Running scripts
- scripts can be run from the command line using:
python -m src.<script_name>

### R365 sync modes

```sh
.venv/bin/python -m src.r365-api-update --daily
.venv/bin/python -m src.r365-api-update --week
.venv/bin/python -m src.r365-api-update --bulk -y 2026
.venv/bin/python -m src.r365-api-update --bulk -y 2026 -p 10
.venv/bin/python -m src.r365-api-update --bulk -y 2026 -p 10 -w 2
.venv/bin/python -m src.r365-api-update --bulk --business-date 2026-10-06
.venv/bin/python -m src.r365-api-update --bulk -y 2026 -p 10 --tables sales_details vendor_invoices
```

- `--daily` updates all enabled endpoints for yesterday. `--daily YYYY-MM-DD`
  selects a specific modification day. Modification timestamp filters cover midnight
  through 23:59:59.999999 with the runtime's local timezone offset. Jobs and
  employees instead require inclusive `YYYY-MM-DD` filters, with both boundaries
  set to the selected date (see the [labor API](https://docs.restaurant365.com/apidocs/public-v1-labor-jobs)).
- `--week` (alias `--weekly`) looks up yesterday's fiscal year, period, and week
  in `core.calendar`, then processes each date in that week through yesterday.
  With the Wednesday–Tuesday fiscal calendar, a Wednesday run processes the
  previous Wednesday through Tuesday; a Saturday run processes Wednesday–Friday.
  Missing calendar dates fail before API requests.
- `--bulk` uses business/document dates, without modification filters. Select a
  fiscal year with `-y/--year`, optionally periods with `-p/--period`, and a week
  within one period with `-w/--fiscal-week`. All fiscal dates come from
  `core.calendar`, including dates falling in a different calendar year.
  Alternatively, use `--business-date YYYY-MM-DD` for a single business date.
  Each selected date is requested separately; unselected periods/gaps are not filled.

Use `--tables` (alias `--sync`) to select endpoints. Daily/week default to
`locations`, `units_of_measure`, `item_categories`, `gl_accounts`, `purchase_items`,
`vendors`, `vendor_items`, `jobs`, `employees`, `users`, `inventory_counts`,
`transactions`, `vendor_invoices`, `daily_sales`, `labor_punches`,
`payroll_summaries`, and `sales_analytics`. Daily sales, payroll summaries, and
sales analytics use business dates; the others use modification filters. Employees and transactions need a full location-ID lookup
for request scoping; unchanged locations must still be included.

**Sales API limitation:** [daily sales](https://docs.restaurant365.com/apidocs/public-v1-sales-daily-sales)
requires `businessDate` and `location`, and does not expose modification filters.
Daily/week refresh sales for the selected business dates. Changes to older sales
business dates require a bulk rerun; daily/week cannot guarantee finding them.

Bulk defaults to `daily_sales`, `transactions`, `vendor_invoices`, `labor_punches`,
`payroll_summaries`, and `sales_analytics`.
`sales_details` and `sales_tickets` are aliases for the complete sales import,
which also maintains `sales_payments`, `sales_ticket_taxes`, and `sales_account`
from the same response. Vendor invoices include `vendor_invoice_details`.
`inventory_counts` also has a business date and may be selected explicitly for
bulk; it is excluded from the default. Reference tables are rejected in bulk.
`--tables all` selects the defaults for the chosen mode. Existing reference data
is retained; bulk does not populate missing reference tables on a fresh database.

Bulk uses the documented business-date filters for
[transactions](https://docs.restaurant365.com/apidocs/public-v1-accounting-transactions)
and [inventory counts](https://docs.restaurant365.com/apidocs/public-v1-inventory-inventory-counts),
and invoice-date filters for
[vendor invoices](https://docs.restaurant365.com/apidocs/public-v1-inventory-invoices).
The existing upsert and complete-sales-snapshot behavior is preserved.

Legacy `daily`, `weekly`, and `bulk` commands remain accepted. A standalone
`--year ... --period ...` selection implies bulk. Numeric `--week N` remains
accepted for older fiscal commands; prefer `-w N` or `--fiscal-week N`.

### Toast sync modes

Toast accepts `--daily`, `--weekly`, or `--period`, updating all enabled endpoints
by default. `--daily` selects yesterday (or an explicit `YYYY-MM-DD`); `--weekly`
selects the last completed fiscal week. `--period` requires `--year`; `--week N`
selects a week within one period from `core.calendar`.

```sh
.venv/bin/python -m src.toast-api-update --daily
.venv/bin/python -m src.toast-api-update --daily 2026-10-08 --sync orders labor
.venv/bin/python -m src.toast-api-update --weekly --sync orders labor
.venv/bin/python -m src.toast-api-update --year 2026 --period 7 8 9 --sync orders
.venv/bin/python -m src.toast-api-update --daily --sync labor --labor-resources time-entries
```

Toast selections include `restaurants`, `orders`, and `labor`; use `--sync` or
`--tables` to select them, or `all` for every endpoint. Labor defaults to all four
endpoints; narrow it with `--labor-resources employees jobs time-entries shifts`.
Restaurants, employees, and jobs refresh once per invocation; orders, time
entries, and shifts use the selected business dates. Toast retains its explicit
business-date commands and restaurants-only no-argument default. Its `--dry-run`
fetches and validates without database writes. See [Toast setup](docs/toast-api-update.md).

### R365 requests and retries

R365 GET requests retry timeouts twice, after 2 and 4 seconds. Vendor invoices
request 50 headers (plus nested details) per page, with a 15-second connection
timeout and a 180-second read timeout on every page and retry. Other endpoints
keep their 60-second timeout. These are inactivity limits, not total runtime limits.
Smaller pages and a longer read timeout can help slow responses but cannot
guarantee availability of the R365 service. Continuation retries request the same page;
exhausted retries raise the error. Vendor invoices and their details are written
only after all pages are fetched. Earlier successful table updates remain
committed. To retry just vendor invoices for September 30–October 6, 2026:

```sh
.venv/bin/python -m src.r365-api-update bulk \
  --year 2026 --period 10 --week 4 --tables vendor_invoices
```

Accounting `invoices` and `invoice_details` updates are commented out in the
dispatcher and excluded from all endpoint selections. Their import functions remain available
for future reactivation; use `vendor_invoices` and `vendor_invoice_details` for
new views. Existing database tables and data are unchanged.

### R365 labor punches, payroll summaries, and sales analytics

Apply [the new labor table setup](db_utils/schema/r365_labor.sql) before using the
new defaults. It creates `labor_punches`, `payroll_summaries`, `sales_analytics`,
and `sales_analytics_jobs`; the importer does not create tables automatically.
See [R365 labor setup, filters, and limitations](docs/r365-labor.md).

### R365 employees, jobs, and users

These syncs upsert existing tables; they do not create or alter schemas or delete
records absent from the response. Employees use `id` as their primary key and
UUID arrays for `other_locations_id` and `other_jobs_id`. POS mappings are written
to `r365.employee_map` using `pos_employee_id` as the conflict key, in the same
transaction as employees. Employee records with missing, blank, or non-UUID POS IDs belong to the archived
POS system and are excluded from both employee and mapping imports, with counts
reported. Existing database rows are not deleted. R365 defines `posEmployeeId` as a string, but the
existing `employee_map.pos_employee_id` column accepts UUIDs only. Duplicate
employee rows with identical mapped values are combined; conflicting values
raise an error before writing.

Users use UUID arrays for `locations_id`, `user_rolls_id`, and `report_rolls_id`.
The API's `canGrantAccessBeyondPersonalLevel` maps to `can_grant_access`.
`all_reports_access` is omitted because the endpoint does not supply it: existing
values are preserved, and new rows use the table's `false` default. Jobs do not
write responsibilities.

### R365 daily sales

For a **new installation**, apply
[`db_utils/schema/r365_daily_sales.sql`](db_utils/schema/r365_daily_sales.sql).
For an **existing ticket-level sales_details table**, apply
[`db_utils/schema/r365_sales_details_daily_migration.sql`](db_utils/schema/r365_sales_details_daily_migration.sql)
instead. Both require PostgreSQL 15+ for null-safe group uniqueness.

Stop scheduled imports while migrating. The migration aggregates all stored
current detail rows belonging to current tickets, preserving every business date
(including dates older than two years), then recreates `r365.sales_details` and
creates `r365.sales_account` in one transaction. It removes the original ticket
line IDs and inactive source rows. It does not call the API or backfill missing
dates. There is no `CASCADE`: dependent views or foreign keys block the migration
and roll back the transaction. Update those consumers before retrying. Reapply
any custom grants on the recreated details table.

```sh
psql --dbname YOUR_DATABASE -v ON_ERROR_STOP=1 -f db_utils/schema/r365_sales_details_daily_migration.sql
```

The six tables are `r365.daily_sales`, `r365.sales_tickets`,
`r365.sales_details`, `r365.sales_account`, `r365.sales_payments`, and
`r365.sales_ticket_taxes`. Ticket, payment, and tax imports remain enabled.

`r365.sales_details` contains one row for each combination of:

- `business_date`, `location_id`
- `pos_item_id`, `pos_item_name`
- `void`, `sales_account_id`
- `menu_item_category_1`, `menu_item_category_2`, `menu_item_category_3`

`sale_amount` and `quantity` are sums of the deduplicated source lines across all
pages/tickets for that day/location. Null grouping values are retained; null
measures are ignored in sums, with an all-null sum remaining null. Voided lines
remain separate and are not subtracted or excluded automatically. The generated
bigint `id` is a local surrogate key, not an R365 detail ID. A
`UNIQUE NULLS NOT DISTINCT` constraint covers all grouping columns, so reruns
also update groups with missing categories or references. Upserts replace totals;
they never add the downloaded totals to the stored totals. Disappearing groups
become inactive only after a complete successful day/location download.

`r365.sales_account` stores the source UUID `id`, `name`, `number`, `gl_type`, and
`last_synced_at`. Join it using `sales_details.sales_account_id = sales_account.id`.
Names/numbers are not keys: separate account IDs remain separate even if their
labels match across locations. Location is carried on the sales totals. Account
attributes reflect the most recently imported values, not historical versions.

The summary, ticket, account, and payment tables use R365 UUID IDs; taxes use
`(sales_ticket_id, tax_index)`, with a zero-based array position. Locations and
servers are stored only as IDs; server names, payroll IDs, location names/numbers,
and free-text ticket comments are excluded from this import. No raw payload is
stored. Existing employee/location imports are unchanged.

Daily summaries are matched by `(location_id, business_date)`. A different
incoming summary UUID means R365 replaced the summary. After validating the
complete API snapshot, the importer deletes the old summary's payments, taxes,
daily item totals, tickets (including inactive rows), and header, then imports
the replacement using its new UUID. Shared `sales_account` records are retained.
The entire replacement is one transaction: failures restore the previous snapshot.
A transaction-scoped location/day lock serializes competing imports. Same-ID
refreshes continue to upsert and mark missing children inactive.

Daily and bulk sales select locations from `core.restaurants` where
`active IS TRUE` and `r365_guid IS NOT NULL`, using `r365_guid` as the API location
ID and the restaurant's `timezone` for timestamp conversion. The R365 locations
API does not determine which restaurants receive daily-sales requests.
No automatic retention cutoff deletes old data. Use the fiscal bulk commands
above to backfill historical periods.

The backfill still requests full tickets from R365 but stores their item totals
at the daily grain. It processes only currently active restaurants; historical
closed restaurants are not selected. Missing API days are reported as skipped,
so a requested fiscal range does not guarantee R365 supplies every day.

Each location/day is fully fetched and validated before one transaction upserts
the summary and children. A repeated summary across pages is stored once.
Incomplete or conflicting pages fail before writes; a failed write rolls back
the entire location/day. Earlier successful location/days remain committed.
An initial 404 skips that location/day without changing stored rows: R365 does
not distinguish missing locations from missing summaries in that response.
A continuation 404 fails the sync rather than treating a partial download as empty.
Explicitly empty/null arrays in a valid complete summary mean no child rows;
missing arrays or pagination metadata are rejected as incomplete.

Ticket children and daily item groups absent from a successful complete snapshot become `is_current = false`;
returned children become current again. Rows are deleted only when replacing a superseded summary as described above. `last_synced_at`
records when a row was last received; inactive rows retain their previous value.
This is current-state synchronization, not a full change-history archive. Tax
slots can be overwritten if the source reorders its array.

Reporting views must filter child tables with `is_current = true`. Aggregate
items, payments, and taxes separately before joining them to avoid multiplying
amounts. Location/server/account IDs can join your existing reference tables;
the setup adds foreign keys only between these sales tables. Daily item totals
join summaries on `(location_id, business_date)` and have no ticket foreign key;
retrieve individual item lines from R365 when needed.

Money, quantities, and rates use PostgreSQL `numeric`. Timestamp offsets are
preserved as instants in `timestamptz`; offset-free timestamps require the
restaurant's `timezone` to be an IANA timezone (for example,
`America/New_York`) or a supported US Windows timezone name. Eastern, Central,
Mountain, US Mountain (Arizona), Pacific, Alaskan, Hawaiian Standard Time, and
UTC are mapped using Unicode CLDR; daylight-saving rules come from the resulting
IANA zone. Unknown zones and ambiguous/nonexistent DST times fail
before writes instead of guessing an instant.

Safe mock-based checks:

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_r365*sync.py'
```

The optional PostgreSQL test requires a fresh isolated instance under
`/tmp/r365-sales-pg-*`, Unix socket port 55439, user `sales_test`, database
`postgres`. Set `R365_TEST_PG_SOCKET` to that socket directory and run
`test_r365_daily_sales_postgres_sync.py`. It creates and retains its test tables;
never point it at an existing database.

## Script Descriptions

| Script                             | Description                                                                                                           |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| **budget-update.py**               | Reads budget data from CSV files, processes it, and writes it to PostgreSQL. Normalizes values and handles conflicts. |
| **calendar-update.py**             | Generates a new fiscal calendar for the next year, saving to both CSV and the database.                               |
| **item-conversion-update.py**      | Normalizes and uploads `PurchaseItems.csv` data to the `item_conversion` table.                                       |
| **menu-engineering-online.py**     | Imports sales mix data and exports menu engineering results for online view.                                          |
| **menu-engineering.py**            | Performs menu engineering analysis on R365/Toast data and outputs an Excel report.                                    |
| **menu-item-mapping.py**           | Checks for unmapped menu items in R365 vs Toast POS.                                                                  |
| **odata-table-update.py**          | Updates static/small tables that rarely change.                                                                       |
| **receiving-by-purchased-item.py** | Tracks purchases and vendor totals for selected items.                                                                |
| **recipe-ingredient-update.py**    | Processes recipe ingredient data and updates PostgreSQL with cost and recipe relationships.                           |
| **uofm-update.py**                 | Uploads units of measure from R365 data.                                                                              |


## Maintenance
- Legacy scripts are in /.archive/
- Views are version-controlled in /db_utils/views/ and can be edited safely
- Add new SQL views by placing a .sql file in /db_utils/views/ — they’ll be recreated automatically

## Developer Notes
### Adding a New SQL View
1. Create a new .sql file in db_utils/views/, named after the view you want to create.
-   Example: stockcount_sales_summary_view.sql
2. Write a standard CREATE OR REPLACE VIEW statement inside the file:
```sql
CREATE OR REPLACE VIEW stockcount_sales_summary_view AS
SELECT category, SUM(total_sales) AS total_sales
FROM stockcount_sales
GROUP BY category;
```
3. Run any update script (or call recreate_all_views() directly) to apply it:
```python
python -m src.recipe-ingredient-update
```
### Adding a New Update Script
Create a new .py file in src/, for example:
```python
# src/my_new_update_script.py
```
2. Follow this pattern:
```python
from db_utils.dbconnect import DatabaseConnection
from db_utils.recreate_views import recreate_all_views

def vendor_update(cur, conn, engine):
    # Your ETL or update logic here
    pass

if __name__ == "__main__":
    with DatabaseConnection() as db:
        vendor_update(db.cur, db.conn, db.engine)
        recreate_all_views(db.conn)
```
3. Run it from the command line:
```python
python -m src.my_new_update_script
```
This ensures:
- consistent database connections
- automatic view recreation
- and a modular, maintainable workflow
