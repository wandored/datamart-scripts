"""
This utility-script is used to check that sales are pulling for all locations.

Read menu_items.csv and ingredients.csv files and return a DataFrame with the following columns:
- menu_item: the name of the menu item
- recipe: the recipe of the menu item
- ingredient: the name of the ingredient
- qty: the quantity of the ingredient needed
- uofm: the unit of measure of the quantity
- ingredient_id: the id of the ingredient
"""

import pandas as pd
import psycopg2
from icecream import ic
from psycopg2.errors import IntegrityError
from sqlalchemy.engine.create import create_engine
from datetime import timedelta
import argparse

from config import Config


def get_menu_recipe():
    df = pd.read_csv("./downloads/menu_items.csv", usecols=["Name", "Recipe"])
    # remove all rows that don't have "Steakhouse" or "Casual"
    df = df[df["Name"].str.contains("Steakhouse") | df["Name"].str.contains("Casual")]
    df["Name"] = df["Name"].str.replace("Steakhouse - ", "")
    df["Name"] = df["Name"].str.replace("Casual - ", "")
    df.rename(columns={"Name": "menu_item", "Recipe": "recipe"}, inplace=True)
    return df


def get_recipe_ingredient():
    df = pd.read_csv(
        "./downloads/ingredients.csv",
        usecols=["Item", "Recipe", "Qty", "UofM", "IngredientId"],
    )
    df.rename(
        columns={
            "Item": "ingredient",
            "Recipe": "recipe",
            "Qty": "qty",
            "UofM": "uofm",
            "IngredientId": "ingredient_id",
        },
        inplace=True,
    )
    return df


# get each unique value in item_name column of inv_items table
def get_unique_item_names():
    conn = psycopg2.connect(
        host=Config.HOST_SERVER,
        database=Config.PSYCOPG2_DATABASE,
        user=Config.PSYCOPG2_USER,
        password=Config.PSYCOPG2_PASS,
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT item_name
        FROM inv_items
        """
    )
    item_names = cur.fetchall()
    item_names = [item_name[0] for item_name in item_names]
    cur.close()
    conn.close()
    return item_names


# get yesterday's sales_count from menuitems table for each item in purchase_items
def get_yesterdays_total_count(item_list, date_range):
    conn = psycopg2.connect(
        host=Config.HOST_SERVER,
        database=Config.PSYCOPG2_DATABASE,
        user=Config.PSYCOPG2_USER,
        password=Config.PSYCOPG2_PASS,
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT store, menuitem, sales_count
        FROM menu_item_sales
        WHERE menuitem IN %s
        AND date = %s
        """,
        (tuple(item_list), date_range),
    )
    yesterdays_total_count = cur.fetchall()
    cur.close()
    conn.close()
    yesterdays_total_count.sort()
    return yesterdays_total_count


def main():
    CURRENT_DATE = pd.Timestamp.now().date()
    YSTDAY = CURRENT_DATE - timedelta(days=1)
    YSTDAY = YSTDAY.strftime("%Y-%m-%d")
    engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)

    # creat and argument parser object
    parser = argparse.ArgumentParser()

    # check for user provide argument
    parser.add_argument("-d", "--date", help="Date to run the script")
    args = parser.parse_args()

    # check for date valid format argument
    if args.date:
        try:
            pd.to_datetime(args.date)
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD")
            parser.print_help()
            return

    # check for date argument
    if args.date:
        YSTDAY = args.date
    else:
        YSTDAY = YSTDAY

    print(f"Running script for {YSTDAY}")

    purchase_items = get_unique_item_names()
    menu_recipe = get_menu_recipe()
    recipe_ingredient = get_recipe_ingredient()

    # merge menu_recipe and recipe_ingredient
    menu_recipe_ingredient = pd.merge(menu_recipe, recipe_ingredient, on="recipe")

    # drop all rows from menu_recipe_ingredient not in purchase_items
    menu_recipe_ingredient = menu_recipe_ingredient[
        menu_recipe_ingredient["ingredient"].isin(purchase_items)
    ]
    menuitem_list = menu_recipe_ingredient["menu_item"].tolist()
    sales_count = get_yesterdays_total_count(menuitem_list, YSTDAY)
    ic(sales_count)
    print(len(sales_count))


if __name__ == "__main__":
    main()
