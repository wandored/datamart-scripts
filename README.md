# DataMart Scripts
Collection of utilities and scripts used to manage and update DataMart tables

## File downloads:
### Restaurant365 Files:
* menu_items_export_r365.csv
* Menu Price Analysis.csv
* Product Mix.csv
* PurchaseItems.csv
* RecipeItems.csv
* UnitOfMeasure.csv
* ingredients.csv
* menu_items.csv
* prep_recipes.csv
* wine_vendor_items.csv (archived script)
* Receiving by Purchased Item.csv (item specific)

### Toast Files:
* MenuItem_Export_toast.csv
* MenuItem_Export-no-mods.csv

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
