"""
A script to analyze and compare menu item prices and recipe costs across locations.

This script connects to a database to retrieve menu item prices and recipe costs,
merges the data, and exports it to an Excel file. It supports filtering by
different item types (beer, wine, food, liquor).

Dependencies:
    - pandas
    - argparse
    - icecream
    - xlsxwriter
    - dbconnect (local module)
"""

from dbconnect import DatabaseConnection
import pandas as pd
import argparse
import icecream as ic
import xlsxwriter


def get_recipe_cost(cur) -> pd.DataFrame:
    """
    Retrieve recipe costs from the database.

    Args:
        cur: Database cursor object for executing queries

    Returns:
        pd.DataFrame: DataFrame containing recipe cost data with columns:
            - id: Location ID
            - location: Location name
            - concept: Business concept
            - menu_item: Name of the menu item
            - recipe_cost: Cost of the recipe
    """
    cur.execute(
        """
      SELECT id, location, concept, menu_item, recipe_cost
    FROM recipe_cost
    WHERE menu_item IS DISTINCT FROM '0'
      """
    )
    recipe_cost = cur.fetchall()
    df = pd.DataFrame(
        recipe_cost,
        columns=["id", "location", "concept", "menu_item", "recipe_cost"],
    )
    return df


def get_menu_items(cur, item_type) -> pd.DataFrame:
    """
    Retrieve menu items and their prices from the database.

    Args:
        cur: Database cursor object for executing queries
        item_type (list): List of sales types to filter by (e.g., ['Beer Sales'])

    Returns:
        pd.DataFrame: DataFrame containing menu item data with columns:
            - id: Location ID
            - location: Location name
            - concept: Business concept
            - menu_item: Name of the menu item
            - category: Item category
            - sales_type: Type of sale
            - price: Average price of the item
    """
    query = """
    SELECT DISTINCT ON (r.id, r.name, r.concept, sd.menuitem, sd.category, sa.sales_type)
        r.id,
        r.name,
        r.concept,
        sd.menuitem,
        sd.category,
        sa.sales_type,
        sum(sd.amount) / sum(sd.quantity) AS price
    FROM sales_detail sd
        JOIN restaurants r ON r.locationid::text = sd.location
        JOIN sales_account sa ON sa.name = sd.salesaccount
    WHERE sd.date >= '2024-01-01'
        AND sa.sales_type = ANY(%s)
    GROUP BY r.id, r.name, r.concept, sd.menuitem, sd.category, sa.sales_type, sd.date
    HAVING sum(sd.amount) > 0
    ORDER BY r.id, r.name, r.concept, sd.menuitem, sd.category, sa.sales_type, sd.date DESC;
    """
    cur.execute(query, (item_type,))
    menu_items = cur.fetchall()
    df = pd.DataFrame(
        menu_items,
        columns=[
            "id",
            "location",
            "concept",
            "menu_item",
            "category",
            "sales_type",
            "price",
        ],
    )
    # read downloads/MenuItem_Export-no-mods.csv and filter all menu_item that are not in file
    toast_menu_items = pd.read_csv(
        "downloads/MenuItem_Export-no-mods.csv", usecols=["Name"], encoding="ISO-8859-1"
    )
    menu_item_list = toast_menu_items["Name"].tolist()
    df = df[df["menu_item"].isin(menu_item_list)]

    return df


def merge_dataframes(df1, df2):
    """
    Merge menu items and recipe costs DataFrames.

    Args:
        df1 (pd.DataFrame): Menu items DataFrame
        df2 (pd.DataFrame): Recipe costs DataFrame

    Returns:
        pd.DataFrame: Merged DataFrame containing menu items with their
                     corresponding recipe costs
    """
    return pd.merge(df1, df2, on=["menu_item", "location", "id", "concept"], how="left")
    # pivot_table = pd.pivot_table(
    #         df_merge,
    #         index=["menu_item"],
    #         columns=['location'],
    #         values=['price', 'recipe_cost'],
    #         aggfunc='mean',
    # )

    # return pivot_table


def write_to_excel(df, file_name, item_type):
    """
    Write the merged DataFrame to an Excel file.

    Args:
        df (pd.DataFrame): DataFrame to write
        file_name (str): Path to the output Excel file
        item_type (list): List of sales types used for sheet naming
    """
    writer = pd.ExcelWriter(file_name, engine="xlsxwriter")
    df.to_excel(writer, sheet_name=item_type[0], index=False)
    writer.close()


if __name__ == "__main__":
    # Create an argument parser object
    parser = argparse.ArgumentParser()

    # Check for user-provided arguments
    parser.add_argument("-b", "--beer", help="Beer Menu Items", action="store_true")
    parser.add_argument("-f", "--food", help="Food Menu Items", action="store_true")
    parser.add_argument("-w", "--wine", help="Wine Menu Items", action="store_true")
    parser.add_argument("-l", "--liquor", help="Liquor Menu Items", action="store_true")
    args = parser.parse_args()

    item_type_map = {
        "beer": ["Beer Sales"],
        "wine": ["Wine Sales"],
        "food": ["Food Sales"],
        "liquor": ["Liquor Sales"],
    }

    item_type = [item_type_map[arg][0] for arg in vars(args) if vars(args)[arg]] or [
        "Food Sales",
        "Liquor Sales",
        "Beer Sales",
        "Wine Sales",
    ]

    print(f"Sales Type Selected: {', '.join(item_type)}\n")

    with DatabaseConnection() as db:
        recipe_cost = get_recipe_cost(db.cur)
        menu_items = get_menu_items(db.cur, item_type)
        df = merge_dataframes(menu_items, recipe_cost)
        write_to_excel(
            df, "/home/wandored/Sync/ReportData/menu_item_price.xlsx", item_type
        )
