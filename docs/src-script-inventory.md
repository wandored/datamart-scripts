# Source Script Inventory

Based on the current code, `src` contains **31 standalone scripts** plus the
`toast_sync` support package. These descriptions reflect the implementation;
the scripts were inspected, not executed, for this inventory.

## API imports and synchronization

| Script | Description |
|---|---|
| `r365-api-update.py` | Main R365 importer: daily modification updates, weekly updates, fiscal bulk imports, and POS/menu CSV imports. |
| `toast-api-update.py` | Main Toast importer for restaurants, orders, labor, and configuration, with daily, weekly, and fiscal-period modes. |
| `bulk-table-update.py` | Older bulk importer for R365 transactions, transaction details, labor, and sales data into public tables. |
| `odata-table-update.py` | Updates older reference tables for GL accounts, jobs, locations, companies, items, and sales accounts. |
| `r365-api-request.py` | Diagnostic utility for inspecting R365 responses; currently exports vendor invoice data. |
| `run_all_updates.py` | Runs a predefined sequence of older reference, recipe, and costing updates, stopping on the first failure. |

## Menu items, recipes, and costing

| Script | Description |
|---|---|
| `menu-item-map.py` | Creates restaurant- and concept-aware links between Toast items, R365 menu items, and R365 POS IDs; previews matches before optional writes. |
| `menu-item-mapping.py` | Older discovery tool that compares Toast names with R365 sales items and prints candidates missing from the existing menu table. |
| `menu-item-table-update.py` | Imports `MenuItems_R365.csv` into the older menu-items table, adding concept, categories, and sales-account information. |
| `toast-menu-items.py` | Traverses Toast menus and groups to populate the older `toast_menu_items` table with UUIDs, concepts, and sales categories. |
| `recipe-table-update.py` | Imports recipe IDs, names, reporting units, and yields from `RecipeItems.csv`. |
| `recipe-ingredient-update.py` | Rebuilds recipe-ingredient relationships, latest ingredient costs, and location-specific recipe costs. |
| `recipe-ingredients-flat.py` | Expands nested prep recipes into base ingredients and rebuilds flattened ingredient and menu-recipe tables. |
| `purchase-item-update.py` | Imports R365 purchase-item definitions, including categories, units, conversions, and active status. |
| `item-conversion-update.py` | Combines purchased-item API data and recipe CSV data to maintain weight, volume, and each-unit conversions. |
| `uofm-update.py` | Updates units of measure and conversion factors; also updates transaction references when unit names change. |
| `weekly-item-base-cost.py` | Calculates weekly ingredient costs per base unit and restaurant, using purchases and stock-count fallback data. |

## Sales, operational, and financial reports

| Script | Description |
|---|---|
| `menu-engineering.py` | Builds Excel menu-engineering reports from product-mix and menu-price-analysis CSVs, including profitability/popularity ratings. |
| `menu-engineering-online.py` | Calculates menu profitability from database product-mix and recipe-cost data; its database-write section is currently commented out. |
| `toast_product_mix.py` | Aggregates Toast order items and modifiers into restaurant-level product-mix records and writes them to `toast_product_mix`. |
| `toast_payments.py` | Exports Toast payment details to CSV and summarizes amounts and counts by restaurant and card-entry mode. |
| `toast-kitchen.py` | Calculates kitchen fulfillment timings, exports a CSV, and writes timing records; currently targets one hardcoded restaurant. |
| `toast-analytics.py` | Prototype Toast analytics request for one hardcoded restaurant; it does not currently save or process the response. |
| `receiving-by-purchased-item.py` | Converts a receiving export into an Excel workbook with vendor, restaurant, product, and detail sheets. |
| `stockcount_category_totals.py` | Exports stock-count quantities and values for “Food Other” prep and purchased items for a selected fiscal week. |
| `profit_loss_comparison.py` | Imports an R365 P&L CSV, adds fiscal-calendar fields, and refreshes that week’s records in `reporting.pnl`. |
| `profit_and_loss_store_report.py` | Generates restaurant Excel P&L reports comparing current- and prior-year results through a selected fiscal week. |

## Calendar, budgets, and utilities

| Script | Description |
|---|---|
| `budget-update.py` | Converts budget workbooks into period-level records and upserts them by restaurant, account, year, and period. |
| `calendar-update.py` | Contains fiscal-calendar rollover logic; currently imports a hardcoded 2026 calendar CSV, with generation disabled. |
| `holiday_table_update.py` | Imports holiday names, dates, keys, and offsets from `holiday_table.csv`. |
| `upload_csv_to_db.py` | Generic utility that appends a CSV to a specified database schema and table. |

## Support modules in `src/toast_sync/`

These are used by `toast-api-update.py`, rather than run individually.

| Module | Description |
|---|---|
| `configuration.py` | Retrieves and normalizes restaurant-scoped Toast configuration, including menu items and dining options. |
| `restaurants.py` | Retrieves and upserts Toast restaurant details. |
| `orders.py` | Retrieves orders and separates checks, selections, payments, discounts, taxes, and related records into source tables. |
| `labor.py` | Imports employees, jobs, employee relationships, time entries, and shifts. |
| `locations.py` | Loads configured Toast restaurant identifiers from `core.restaurants`. |
| `dates.py` | Provides shared business-date arguments and validation. |
| `values.py` | Validates and converts UUIDs, timestamps, numbers, and nested source values. |
| `writing.py` | Provides shared transactional bulk-upsert logic. |
| `__init__.py` | Defines the Toast synchronization package. |

## Compatibility note

Several older scripts still reference `db.engine`, which the current
`DatabaseConnection` does not provide; their presence does not mean they are
currently runnable without updates.
