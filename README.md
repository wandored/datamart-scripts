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
.venv/bin/python -m src.r365-api-update daily
.venv/bin/python -m src.r365-api-update weekly
.venv/bin/python -m src.r365-api-update bulk --year 2026 --period 9 --tables daily_sales
.venv/bin/python -m src.r365-api-update bulk --year 2026 --period 9 --week 5 --tables daily_sales vendor_invoices
.venv/bin/python -m src.r365-api-update bulk --year 2026 --period 7 8 9 --tables daily_sales inventory_counts
```

An explicit mode is required. These commands replace the old resource flags and
manual date arguments; update scheduled commands accordingly.

- `daily` imports the fiscal week containing **yesterday**, from that week's first
  date through yesterday inclusive. It reads `year`, `period`, and `week` from `core.calendar`,
  using the runtime's local date. On fiscal day 1, it imports the previous fiscal
  week's days 1–7; on day 4, it imports the current week's days 1–3.
- `weekly` refreshes locations, units of measure, item categories, GL accounts,
  purchase items, vendors, vendor items, jobs, employees (with POS mappings), and
  users. Modification/creation timestamps and employee hire dates do not make
  these reference records daily data. Jobs and employees require API date filters;
  the refresh requests modification dates from `0001-01-01` through today.
  Employees are scoped to all locations returned by the locations API.
- `bulk` imports only the required `--tables` selection for fiscal `year` and one
  or more `period` values. Choose one or more of `daily_sales`, `inventory_counts`,
  `transactions`, and `vendor_invoices`; there is no default selection.
  Add `--week` to select a week within a single period. Week numbers restart within each
  period: usually 1–4, with week 5 when present in `core.calendar`. For example,
  fiscal year 2026 period 9 has five weeks; its other twelve periods have four.
  Dates are read from `core.calendar`; unselected periods and calendar gaps are
  never filled in. A selection with no matching dates fails before API calls.

Daily imports all dated tables; bulk selects from the same groups: daily sales and their child tables,
including sales accounts, inventory counts, transactions, and vendor invoices
and their details. All use inclusive record-date
filters, not modification dates: sales/counts/transactions use business dates,
and vendor invoices use invoice dates. Reference
records embedded in sales responses are kept with their sales sync.

The API date filters follow the R365 documentation for
[transactions](https://docs.restaurant365.com/apidocs/public-v1-accounting-transactions),
[vendor invoices](https://docs.restaurant365.com/apidocs/public-v1-inventory-invoices), and
[jobs](https://docs.restaurant365.com/apidocs/public-v1-labor-jobs).
Older corrections outside the daily fiscal window require a bulk rerun.

R365 GET requests retry timeouts twice, after 2 and 4 seconds, keeping the
60-second timeout per attempt. Continuation retries request the same page;
exhausted retries raise the error. Vendor invoices and their details are written
only after all pages are fetched. Earlier successful table updates remain
committed. To retry just vendor invoices for September 30–October 6, 2026:

```sh
.venv/bin/python -m src.r365-api-update bulk \
  --year 2026 --period 10 --week 4 --tables vendor_invoices
```

Accounting `invoices` and `invoice_details` updates are commented out in the
dispatcher and excluded from bulk choices. Their import functions remain available
for future reactivation; use `vendor_invoices` and `vendor_invoice_details` for
new views. Existing database tables and data are unchanged.

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
returned children become current again. No rows are deleted. `last_synced_at`
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
