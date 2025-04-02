"""
ingredient-update processes recipe ingredient data and stores it in a PostgreSQL database.
It reads data from CSV files, transforms it, and updates the database with the processed data.
The script is designed to work with menu items and their corresponding recipes,
filtering specific menu types, and adjusting ingredient quantities based on units of measure.
"""

from datetime import datetime

import pandas as pd
from psycopg2.errors import IntegrityError

from dbconnect import DatabaseConnection
from utils import (
    recreate_stockcount_monthly_view,
    recreate_stockcount_purchases_view,
    recreate_stockcount_sales_view,
    recreate_stockcount_waste_view,
)


def get_recipe_ingredient() -> pd.DataFrame:
    df = pd.read_csv(
        "./downloads/ingredients.csv", usecols=["Item", "Recipe", "Qty", "UofM"]
    )
    df.rename(
        columns={
            "Item": "ingredient",
            "Recipe": "recipe",
            "Qty": "qty",
            "UofM": "uofm",
        },
        inplace=True,
    )
    return df


def get_prep_recipes() -> pd.DataFrame:
    df = pd.read_csv(
        "./downloads/prep_recipes.csv",
        usecols=["Name", "YieldUofM", "YieldQty"],
    )
    df.rename(
        columns={"Name": "recipe", "YieldUofM": "uofm", "YieldQty": "quantity"},
        inplace=True,
    )
    return df


def get_uofm(cur) -> pd.DataFrame:
    cur.execute(
        """
        SELECT name, base_uofm, base_qty
        FROM unitsofmeasure
        """
    )
    uofm = cur.fetchall()
    df = pd.DataFrame(uofm, columns=["name", "base_uofm", "base_qty"])
    df.rename(
        columns={"name": "uofm", "base_uofm": "base_uofm", "base_qty": "base_qty"},
        inplace=True,
    )
    return df


def get_item_conversion(cur) -> pd.DataFrame:
    cur.execute(
        """
        SELECT name, weight_qty, weight_uofm, volume_qty, volume_uofm, each_qty, each_uofm, measure_type
        FROM item_conversion
        """
    )
    item_conversion = cur.fetchall()
    df = pd.DataFrame(
        item_conversion,
        columns=[
            "name",
            "weight_qty",
            "weight_uofm",
            "volume_qty",
            "volume_uofm",
            "each_qty",
            "each_uofm",
            "measure_type",
        ],
    )
    df.rename(columns={"name": "ingredient"}, inplace=True)
    return df


def calculate_menu_cost(row):
    if row["uofm"] == row["base_uofm"]:
        return row["qty"] * row["base_cost"]
    else:
        if row["uofm"] == "OZ-fl" and row["base_uofm"] == "OZ-wt":
            conversion_factor = row["volume_qty"] / row["weight_qty"]
        elif row["uofm"] == "OZ-wt" and row["base_uofm"] == "OZ-fl":
            conversion_factor = row["weight_qty"] / row["volume_qty"]
        elif row["uofm"] == "Each" and row["base_uofm"] == "OZ-wt":
            conversion_factor = row["each_qty"] / row["weight_qty"]
        elif row["uofm"] == "Each" and row["base_uofm"] == "OZ-fl":
            conversion_factor = row["each_qty"] / row["volume_qty"]
        elif row["uofm"] == "OZ-wt" and row["base_uofm"] == "Each":
            conversion_factor = row["weight_qty"] / row["each_qty"]
        elif row["uofm"] == "OZ-fl" and row["base_uofm"] == "Each":
            conversion_factor = row["volume_qty"] / row["each_qty"]
        else:
            print(row["ingredient"], "Conversion not found")
            conversion_factor = (
                1  # Handle cases where no conversion is needed or available
            )
        return row["qty"] * conversion_factor * row["base_cost"]


def get_menu_recipe():
    df = pd.read_csv("./downloads/menu_items.csv", usecols=["Name", "Recipe"])
    # remove all rows that don't have "Steakhouse" or "Casual"
    df = df[df["Name"].str.contains("Steakhouse") | df["Name"].str.contains("Casual")]
    # split Name column into two columns
    df[["concept", "menu_item"]] = df["Name"].str.split(" - ", expand=True)
    # df["Name"] = df["Name"].str.replace("Steakhouse - ", "")
    # df["Name"] = df["Name"].str.replace("Casual - ", "")
    df.rename(columns={"Recipe": "recipe"}, inplace=True)
    return df


def ingredient_update(cur, conn, engine) -> None:
    recipe_ingredient = get_recipe_ingredient()
    uofm = get_uofm(cur)
    menu_recipe = get_menu_recipe()

    # replace prep recipe with purchase item
    recipe_ingredient["ingredient"] = recipe_ingredient["ingredient"].replace(
        {
            "PREP Marination Sirloin (10 oz-wt)": "BEEF Steak 10oz Sirloin Choice",
            "PREP Marination Sirloin (11 oz-wt)": "BEEF Steak 11oz Sirloin Choice",
            "PREP Prime Rib Seasoned/Cooked": "BEEF Export Rib Prime",
            "PREP Cajun Steak 12oz Portion": "BEEF Export Rib Prime",
            "PREP Marination Pork Chop Double Cut": "PORK Rib Chop 2 Bone",
            "PREP Pork Chop Portioning 6oz": "PORK Loin Boneless",
            "PREP Marination Chicken Wings": "PLTRY Chicken Wing Jumbo",
        },
        # Add more replacements
    )

    recipe_ingredient = recipe_ingredient.merge(uofm, on="uofm", how="left")
    recipe_ingredient = recipe_ingredient.merge(menu_recipe, on="recipe", how="left")
    recipe_ingredient["qty"] = recipe_ingredient["qty"] * recipe_ingredient["base_qty"]
    recipe_ingredient.drop(columns=["uofm", "base_qty"], inplace=True)
    recipe_ingredient.rename(columns={"base_uofm": "uofm"}, inplace=True)
    recipe_ingredient = recipe_ingredient[
        ["concept", "menu_item", "recipe", "ingredient", "qty", "uofm"]
    ]
    recipe_ingredient.sort_values(
        by=["concept", "menu_item", "recipe", "ingredient"], inplace=True
    )
    # drop all items where concept is null
    recipe_ingredient = recipe_ingredient[recipe_ingredient["concept"].notnull()]

    # drop table if exists
    cur.execute('drop table if exists "recipe_ingredients" cascade')
    conn.commit()
    # update database with table
    try:
        recipe_ingredient.to_sql("recipe_ingredients", engine, index=False)
        conn.commit()
    except IntegrityError:
        print("Error writing to database: IntegrityError")
    except Exception as e:
        print("Error writing to database:", e)

    with open("/home/wandored/Sync/ReportData/recipe_ingredients.csv", "w") as f:
        recipe_ingredient.to_csv(f, index=False)

    return recipe_ingredient


def update_ingredient_cost(cur, conn, engine, recipes) -> pd.DataFrame:
    """
    Updates the ingredient cost information in the database by calculating the base cost for both individual
    ingredients and prepared recipes, and then stores the data in a new 'ingredient_cost' table.

    This function performs the following steps:
    1. Retrieves a list of unique recipes that start with "PREP " from the `recipes` table.
    2. Queries the database to get the latest purchase data for each item, calculating the base cost based on the unit of measure.
    3. For each prepared recipe, calculates the total cost by summing up the costs of its ingredients.
    4. Merges the calculated costs with recipe and unit of measure data to determine the base cost for each prepared recipe.
    5. Combines the prepared recipe costs with the individual ingredient costs into a single DataFrame.
    6. Drops the existing 'ingredient_cost' table from the database, if it exists.
    7. Inserts the new ingredient cost data into the 'ingredient_cost' table in the database, handling any integrity or general exceptions.

    Args:
        cur: Database cursor object for executing SQL commands.
        conn: Database connection object used to commit changes to the database.
        engine: SQLAlchemy engine object for connecting to the database.
        recipes: DataFrame containing recipe information, including ingredient names and quantities.

    Returns:
        pd.DataFrame: A DataFrame containing the updated ingredient cost data, including columns for item, store ID, store name, date, amount, unit of measure, quantity, base cost, and base unit of measure.

    Raises:
        IntegrityError: If there is an integrity issue while writing to the database.
        Exception: For any other errors encountered during the database update process.
    """

    # get list of unique recipes that begin with "PREP " from recipes table
    prep_recipes = get_prep_recipes()
    prep_list = prep_recipes["recipe"].unique().tolist()

    cur.execute(
        """
        WITH latestpurchases AS (
         SELECT p.id,
            p.store,
            p.item,
            p.uofm,
            p.quantity,
            p.amount,
            p.date,
            row_number() OVER (PARTITION BY p.store, p.item ORDER BY p.date DESC) AS rn
           FROM purchases p
        )
         SELECT lp.date,
            lp.id,
            lp.store,
            lp.item,
            lp.quantity,
            lp.uofm,
            lp.amount,
            um.base_uofm,
            um.base_qty,
            lp.amount / lp.quantity / um.base_qty AS base_cost
           FROM latestpurchases lp
             JOIN unitsofmeasure um ON um.name = lp.uofm
          WHERE lp.rn = 1 AND lp.quantity <> 0::double precision
          ORDER BY lp.item, lp.date DESC;
        """
    )
    query = cur.fetchall()
    item_cost = pd.DataFrame(
        query,
        columns=[
            "date",
            "id",
            "store",
            "item",
            "quantity",
            "uofm",
            "amount",
            "base_uofm",
            "base_qty",
            "base_cost",
        ],
    )

    prep_recipe_costs = pd.DataFrame()
    for prep in prep_list:
        prep_items = recipes[recipes["recipe"] == prep]
        prep_items = prep_items[["ingredient", "qty"]]
        prep_items = prep_items.merge(item_cost, left_on="ingredient", right_on="item")
        prep_items["amount"] = prep_items["base_cost"] * prep_items["qty"]
        prep_cost = prep_items.groupby(["id", "store"])["amount"].sum().reset_index()
        prep_cost["recipe"] = prep
        # reorder columns
        prep_cost = prep_cost[["recipe", "id", "store", "amount"]]
        prep_recipe_costs = pd.concat([prep_recipe_costs, prep_cost])

    prep_recipe_costs = prep_recipe_costs.merge(prep_recipes, on="recipe", how="left")
    uofm = get_uofm(cur)
    prep_recipe_costs = prep_recipe_costs.merge(uofm, left_on="uofm", right_on="uofm")
    prep_recipe_costs["base_cost"] = (
        prep_recipe_costs["amount"] / prep_recipe_costs["base_qty"]
    )
    prep_recipe_costs["date"] = datetime.now().date()
    prep_recipe_costs.rename(columns={"recipe": "item"}, inplace=True)
    prep_recipe_costs = prep_recipe_costs[
        [
            "date",
            "id",
            "store",
            "item",
            "quantity",
            "uofm",
            "amount",
            "base_uofm",
            "base_qty",
            "base_cost",
        ]
    ]

    df = pd.concat([prep_recipe_costs, item_cost])
    df.rename(columns={"id": "store_id"}, inplace=True)
    df = df[
        [
            "item",
            "store_id",
            "store",
            "date",
            "amount",
            "uofm",
            "quantity",
            "base_cost",
            "base_uofm",
            "base_qty",
        ]
    ]
    df.sort_values(by=["item", "store", "date"], inplace=True)

    # drop table if exists
    cur.execute('drop table if exists "ingredient_cost" cascade')
    conn.commit()
    # update database with table
    try:
        df.to_sql("ingredient_cost", engine, index=False)
        conn.commit()
    except IntegrityError:
        print("Error writing to database: IntegrityError")
    except Exception as e:
        print("Error writing to database:", e)

    return df


def update_recipe_cost(cur, conn, engine) -> None:
    """
    Fetches location and restaurant data from a database, merges it with menu analysis data,
    and updates the database with a new table containing recipe cost information.

    This function performs the following steps:
    1. Retrieves location data from the 'location' table in the database.
    2. Retrieves restaurant data from the 'restaurants' table in the database.
    3. Merges the location data with the restaurant data based on 'locationid'.
    4. Reads menu analysis data from a CSV file, renames columns for consistency, and merges it with the restaurant data.
    5. Processes the merged data to extract 'concept' and 'menu_item' information, adds a 'date' column, and fills any missing values with 0.
    6. Drops any existing 'recipe_cost' table in the database.
    7. Attempts to update the database with the new 'recipe_cost' table, handling any database integrity errors.

    Returns:
        None

    Raises:
        IntegrityError: If there is an integrity issue while writing to the database.
        Exception: For any other errors encountered during the database update process.
    """

    cur.execute("SELECT locationid, name FROM location")
    location = cur.fetchall()
    location = pd.DataFrame(location, columns=["locationid", "name"])

    cur.execute("SELECT locationid, name, id FROM restaurants")
    restaurants = cur.fetchall()
    restaurants = pd.DataFrame(restaurants, columns=["locationid", "name", "id"])
    restaurants.rename(columns={"name": "location"}, inplace=True)
    restaurants.dropna(inplace=True)

    df = pd.merge(restaurants, location, on="locationid", how="left")
    df.drop(columns=["locationid"], inplace=True)

    menu_analysis = pd.read_csv(
        "./downloads/Menu Price Analysis.csv",
        skiprows=3,
        sep=",",
        thousands=",",
        usecols=["MenuItemName", "Location", "UnitCost_Loc"],
    )
    menu_analysis.rename(
        columns={
            "MenuItemName": "menu_item",
            "Location": "name",
            "UnitCost_Loc": "recipe_cost",
        },
        inplace=True,
    )
    menu_analysis["name"] = menu_analysis["name"].str.strip()
    df = pd.merge(menu_analysis, df, on="name", how="left", sort=False)
    df.drop(columns=["name"], inplace=True)
    df[["concept", "menu_item"]] = df["menu_item"].str.split(" - ", n=1, expand=True)
    df["date"] = datetime.now().date()
    df = df.fillna(0)
    df = df[["date", "id", "location", "concept", "menu_item", "recipe_cost"]]
    # drop table if exists
    cur.execute('drop table if exists "recipe_cost" cascade')
    conn.commit()
    # update database with table
    try:
        df.to_sql("recipe_cost", engine, index=False)
        conn.commit()
    except IntegrityError:
        print("Error writing to database: IntegrityError")
    except Exception as e:
        print("Error writing to database:", e)

    return


if __name__ == "__main__":
    with DatabaseConnection() as db:
        recipe_ingredients = ingredient_update(db.cur, db.conn, db.engine)
        ingredient_costs = update_ingredient_cost(
            db.cur, db.conn, db.engine, recipe_ingredients
        )
        update_recipe_cost(db.cur, db.conn, db.engine)
        recreate_stockcount_sales_view(db.conn)
        recreate_stockcount_waste_view(db.conn)
        recreate_stockcount_purchases_view(db.conn)
        recreate_stockcount_monthly_view(db.conn)
