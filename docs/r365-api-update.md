# R365 API updates

`src.r365-api-update` dispatches modules under `src/r365_sync`, following the
Toast updater's resource registry and `sync` entry point pattern. Table
transformations live in individual modules. API retrieval still uses
`db_utils.r365_importers` and `R365Client`; all ordinary table writes share
`src/r365_sync/writing.py`.

## Daily and reference runs

Run daily resources with:

```sh
.venv/bin/python -m src.r365-api-update
# Equivalent explicit selection:
.venv/bin/python -m src.r365-api-update --sync daily
```

**The default now runs every daily resource.** Previously, most calls were
commented out and the default ran only locations and daily sales. All selected
target tables must already exist; the updater does not create or migrate schemas.

| Group | Resources |
| --- | --- |
| `daily` (default) | `locations`, `purchase_items`, `vendor_items`, `inventory_counts`, `transactions`, `invoices`, `vendor_invoices`, `employees`, `users`, `daily-sales` |
| `reference` | `units_of_measure`, `item_categories`, `gl_accounts`, `vendors`, `jobs` |
| `all` | Both groups |

Refresh reference tables on demand or with a separate, less frequent scheduled
command. Jobs require modification-date filters, so `reference` and `all`
**require both** `--modified-on-start` and `--modified-on-end`. Choose a range
covering the period since the last successful reference refresh, including an
overlap if appropriate. For an initial load, choose a start covering your history.
For example:

```sh
.venv/bin/python -m src.r365-api-update --sync reference \
  --modified-on-start 2026-09-29 --modified-on-end 2026-10-06

.venv/bin/python -m src.r365-api-update --sync all \
  --modified-on-start 2026-09-29 --modified-on-end 2026-10-06
```

These example dates are fixed, not rolling scheduler expressions. The updater
neither installs a schedule nor remembers the last refresh. A scheduler must
supply the intended date window. The other reference resources are fetched in
full and upserted. An `all` run applies the labor date range to employees too.

Select individual resources or combine them with groups:

```sh
.venv/bin/python -m src.r365-api-update --sync vendors units_of_measure
.venv/bin/python -m src.r365-api-update --sync daily vendors
.venv/bin/python -m src.r365-api-update --sync invoices vendor_invoices
.venv/bin/python -m src.r365-api-update --full-vendor-items
```

Selections are deduplicated and run in a fixed dependency order, with selected
reference resources before their consumers. Selecting a table does not implicitly
refresh its dependencies. Related children are always handled with their parent:

| Resource | Tables handled together |
| --- | --- |
| `invoices` | `invoices`, `invoice_details` |
| `vendor_invoices` | `vendor_invoices`, `vendor_invoice_details` |
| `employees` | `employees`, `employee_map` |
| `daily-sales` | `daily_sales`, `sales_tickets`, `sales_account`, `sales_details`, `sales_payments`, `sales_ticket_taxes` |

Child tables have their own transformation modules but cannot be dispatched
separately: their parent workflow preserves the existing atomic transaction.
Daily sales continue to use one transaction per location/business date.

## Existing date behavior

This refactor preserves the retrieval windows:

| Resource | Window |
| --- | --- |
| `employees`, `jobs` | Modified today by default; `--modified-on-start` / `--modified-on-end` override. For explicit individual selections, one boundary still selects only that day. |
| `daily-sales` | Previous seven completed business dates; `--business-date-start` / `--business-date-end` override. One boundary selects only that day. |
| `invoices`, `vendor_invoices`, `vendor_items` | Modified today in the runtime's local timezone. `--full-vendor-items` fetches all vendor items and runs only that resource. |
| `inventory_counts` | Previous seven days through today. |
| `transactions` | Yesterday through today, using the existing modification window. |
| Other resources | Full API retrieval with upserts. |

Labor date flags do not change invoice, vendor-item, inventory-count, or
transaction windows. Existing business-date flags and the `daily-sales` resource
name are unchanged. Daily sales use active mapped restaurants from
`core.restaurants`; employees and transactions use locations from the R365 API.

## Failure and write behavior

The dispatcher logs each selected resource. A resource failure is reported by
name and exception type, remaining resources are attempted, and the process exits
with status `1`. Initialization failures also return `1`; invalid arguments return
`2`. Successful runs return `0`. Fetch/upsert counts remain in the resource output.
Exception messages are not logged because they may contain private API/database
values. A successful resource is not rolled back if another resource fails.

Existing primary/conflict keys, null handling, pagination, and transaction scopes
are preserved. No truncate/reload operations are introduced. Daily sales retain
their scoped `is_current` reconciliation. No live API or database changes are
needed to install this refactor.
