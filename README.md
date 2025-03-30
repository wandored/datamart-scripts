# DataMart Scripts
Collection of utilities and scripts used to manage and update DataMart tables

## File downloads:
### Restaurant365 Files:
* Menu Price Analysis.csv
* Product Mix.csv
* Receiving by Purchased Item.csv
* UnitOfMeasure.csv
* menu_items_export_r365.csv

### Toast Files:
* MenuItem_Export-no-mods.csv
* MenuItem_Export_toast.csv

### Company Files:
* budgets/*.csv (store budgets)
* fiscal_calendar_2024.csv
* specialty.txt

## Utility Files:
* config.py
* dbconnect.py
* utils.py

## Scripts
* **budget-update.py**
  * This script reads budget data from CSV files, processes it into a structured format, and writes the processed data to a PostgreSQL database. It extracts budget values for different periods, normalizes the data, and updates the database while handling conflicts.
* **calendar-update.py**
  * This script automates the creation of a new fiscal calendar for the following year based on an existing fiscal calendar. It processes the data from a CSV file, updates the dates, and writes the updated calendar to both a new CSV file and a PostgreSQL database.
* **item-conversion-update.py**
  * This script reads data from (`PurchaseItems.csv`), processes it by renaming columns for consistency, and then writes the data into table named `item_conversion`.
* **item-price-check.py**
  * A script to analyze and compare menu item prices and recipe costs across locations.
* **menu-engineering-online.py**
  * Import sales mix and export menu engineering report to an excel table for online view.
* **menu-engineering.py**
  * This script processes restaurant menu data from CSV files, performs menu engineering analysis, and formats the results into an Excel report.
* **menu-item-mapping.py**
  * Utility used to check for Menu Items not mapped in R365 from Toast POS
* **receiving-by-purchased-item.py**
  * Track purchases of select items in the receiving by purchased items report.  Sorting Products and Vendors and calculating totals for each.
* **recipe-ingredient-update.py**
  * ingredient-update processes recipe ingredient data and stores it in a PostgreSQL database. It reads data from CSV files, transforms it, and updates the database with the processed data. The script is designed to work with menu items and their corresponding recipes, filtering specific menu types, and adjusting ingredient quantities based on units of measure.
* **sales-check.py**
  * This utility-script is used to check that sales are pulling for all locations.
* **unlinked-recipes.py**
  * Identify all item, prep_recipes and menu_recipes not linked to an active menu_item
* **uofm-update.py**
  * Upload R365 units of measure to datamart
