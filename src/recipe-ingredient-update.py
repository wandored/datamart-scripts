"""
ingredient-update processes recipe ingredient data and stores it in a PostgreSQL database.
It reads data from CSV files, transforms it, and updates the database with the processed data.
The script is designed to work with menu items and their corresponding recipes,
filtering specific menu types, and adjusting ingredient quantities based on units of measure.
"""

from datetime import datetime, timedelta

import pandas as pd
from psycopg2.errors import IntegrityError
from collections import defaultdict

from db_utils.dbconnect import DatabaseConnection
from db_utils.recreate_views import recreate_all_views


def get_recipe_ingredient() -> pd.DataFrame:
    df = pd.read_csv(
        "./downloads/ingredients.csv", usecols=["Item", "Recipe", "Qty", "UofM"]
    )
    df = df.rename(
        columns={
            "Item": "ingredient",
            "Recipe": "recipe",
            "Qty": "qty",
            "UofM": "uofm",
        },
    )
    return df


def get_prep_recipes() -> pd.DataFrame:
    df = pd.read_csv("./downloads/RecipeItems.csv")
    # remove all rows where Category 1 is not "Prep Recipe"
    df = df[df["Category 1"] == "Prep Recipe"]
    df = df[["Name", "Yield UofM", "Yield Qty"]]
    df = df.rename(
        columns={"Name": "recipe", "Yield UofM": "uofm", "Yield Qty": "quantity"},
    )
    return df


def get_uofm(db) -> pd.DataFrame:
    db.execute(
        """
        SELECT name, base_uofm, base_qty
        FROM unitsofmeasure
        """
    )
    uofm = db.fetchall()
    df = pd.DataFrame(uofm, columns=["name", "base_uofm", "base_qty"])
    df = df.rename(
        columns={"name": "uofm", "base_uofm": "base_uofm", "base_qty": "base_qty"},
    )
    return df


def get_item_conversion(db) -> pd.DataFrame:
    db.execute(
        """
        SELECT name, weight_qty, weight_uofm, volume_qty, volume_uofm, each_qty, each_uofm, measure_type
        FROM item_conversion
        """
    )
    item_conversion = db.fetchall()
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
    df = df.rename(columns={"name": "ingredient"})
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
    df = pd.read_csv("./downloads/MenuItems_R365.csv")
    # drop all rows where Category 1 is null
    df = df.dropna(subset=["Category 1"])
    # keep only Name and Recipe columns
    df = df[["Name", "Recipe"]]
    # split Name column into two columns
    df[["concept", "menu_item"]] = df["Name"].str.split(" - ", expand=True)
    df = df.rename(columns={"Recipe": "recipe"})
    return df


# def ingredient_update_flat(db) -> pd.DataFrame:
#     recipe_ingredient = get_recipe_ingredient()
#     uofm = get_uofm(db)
#     menu_recipe = get_menu_recipe()
#     recipe_items = get_prep_recipes()
#
#     # Normalize names same way as recipe column
#     recipe_items["recipe"] = recipe_items["recipe"].str.strip()
#     recipe_names = set(
#         recipe_items["recipe"].unique()
#     )  # for quick lookup during recursion
#
#     # Merge with UOFM to convert yield to base
#     recipe_items = recipe_items.merge(
#         uofm,
#         left_on="uofm",
#         right_on="uofm",
#         how="left",
#     )
#
#     recipe_items["yield_base_qty"] = recipe_items["quantity"] * recipe_items["base_qty"]
#
#     recipe_yield_map = dict(zip(recipe_items["recipe"], recipe_items["yield_base_qty"]))
#
#     # ---------------------------------------------------------
#     # 1️⃣ Convert all recipe quantities to base UOFM
#     # ---------------------------------------------------------
#
#     recipe_ingredient = recipe_ingredient.merge(uofm, on="uofm", how="left")
#     recipe_ingredient["qty"] = recipe_ingredient["qty"] * recipe_ingredient["base_qty"]
#
#     recipe_ingredient = recipe_ingredient.drop(columns=["uofm", "base_qty"])
#     recipe_ingredient = recipe_ingredient.rename(columns={"base_uofm": "uofm"})
#
#     # ---------------------------------------------------------
#     # 2 Build recipe graph for recursion
#     # ---------------------------------------------------------
#
#     recipe_map = defaultdict(list)
#
#     for _, row in recipe_ingredient.iterrows():
#         recipe_map[row["recipe"]].append(
#             {
#                 "ingredient": row["ingredient"],
#                 "qty": row["qty"],
#                 "uofm": row["uofm"],
#             }
#         )
#
#     # ---------------------------------------------------------
#     # 4️⃣ Recursive PREP explosion
#     # ---------------------------------------------------------
#
#     def explode_recipe(recipe_name, multiplier=1.0, visited=None):
#         if visited is None:
#             visited = set()
#
#         if recipe_name in visited:
#             raise ValueError(f"Circular recipe detected: {recipe_name}")
#
#         visited.add(recipe_name)
#
#         components = recipe_map.get(recipe_name, [])
#         results = []
#
#         # Get yield (default = 1 if missing)
#         recipe_yield = recipe_yield_map.get(recipe_name, 1.0)
#
#         for comp in components:
#             ingredient = comp["ingredient"]
#             qty = comp["qty"]
#             uofm = comp["uofm"]
#
#             # 🔹 Adjust for yield
#             effective_multiplier = multiplier * (qty / recipe_yield)
#
#             if ingredient in recipe_names:
#                 results.extend(
#                     explode_recipe(
#                         ingredient,
#                         multiplier=effective_multiplier,
#                         visited=visited.copy(),
#                     )
#                 )
#             else:
#                 results.append(
#                     {
#                         "ingredient": ingredient,
#                         "qty": effective_multiplier,
#                         "uofm": uofm,
#                     }
#                 )
#
#         return results
#
#     # ---------------------------------------------------------
#     # 3 Attach concept + primary menu item
#     # ---------------------------------------------------------
#
#     recipe_ingredient = recipe_ingredient.merge(menu_recipe, on="recipe", how="left")
#
#     recipe_ingredient = recipe_ingredient[recipe_ingredient["concept"].notna()]
#
#     # ---------------------------------------------------------
#     # 5️⃣ Explode only MENU recipes
#     # ---------------------------------------------------------
#
#     flattened_rows = []
#
#     # menu_rows = recipe_ingredient[~recipe_ingredient["recipe"].str.startswith("PREP ")]
#     menu_recipes = recipe_ingredient[
#         ~recipe_ingredient["recipe"].str.startswith("PREP ")
#     ][["concept", "menu_item", "recipe"]].drop_duplicates()
#
#     for _, row in menu_recipes.iterrows():
#         concept = row["concept"]
#         menu_item = row["menu_item"]
#         recipe_name = row["recipe"]
#
#         exploded = explode_recipe(recipe_name)
#
#         for comp in exploded:
#             flattened_rows.append(
#                 {
#                     "concept": concept,
#                     "menu_item": menu_item,
#                     "recipe": recipe_name,
#                     "ingredient": comp["ingredient"],
#                     "qty": comp["qty"],
#                     "uofm": comp["uofm"],
#                 }
#             )
#
#     flat_df = pd.DataFrame(flattened_rows)
#
#     # ---------------------------------------------------------
#     # 6️⃣ Aggregate duplicate ingredients
#     # ---------------------------------------------------------
#
#     flat_df = flat_df.groupby(
#         ["concept", "menu_item", "recipe", "ingredient", "uofm"],
#         as_index=False,
#     ).sum()
#
#     flat_df = flat_df.sort_values(by=["concept", "menu_item", "ingredient"])
#
#     # ---------------------------------------------------------
#     # 7️⃣ Write to database
#     # ---------------------------------------------------------
#
#     try:
#         db.execute('truncate table "recipe_ingredients_flat"')
#         db.commit()
#         flat_df.to_sql(
#             "recipe_ingredients_flat",
#             db.engine,
#             index=False,
#             if_exists="append",
#         )
#     except Exception:
#         db.rollback()
#         try:
#             flat_df.to_sql(
#                 "recipe_ingredients_flat",
#                 db.engine,
#                 index=False,
#                 if_exists="replace",
#             )
#         except Exception as e:
#             print("Error writing to database:", e)
#
#     return flat_df


def ingredient_update(db) -> pd.DataFrame:
    recipe_ingredient = pd.DataFrame(get_recipe_ingredient())
    uofm = pd.DataFrame(get_uofm(db))
    menu_recipe = pd.DataFrame(get_menu_recipe())

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
    recipe_ingredient = recipe_ingredient.drop(columns=["uofm", "base_qty"])
    recipe_ingredient = recipe_ingredient.rename(columns={"base_uofm": "uofm"})
    recipe_ingredient = recipe_ingredient[
        ["concept", "menu_item", "recipe", "ingredient", "qty", "uofm"]
    ]
    recipe_ingredient = recipe_ingredient.sort_values(
        by=["concept", "menu_item", "recipe", "ingredient"]
    )
    # drop all items where concept is null
    recipe_ingredient = recipe_ingredient[recipe_ingredient["concept"].notna()]

    # truncate table and re-insert to preserve table structure and constraints
    try:
        db.execute('truncate table "recipe_ingredients"')
        db.commit()
        recipe_ingredient.to_sql(
            "recipe_ingredients", db.engine, index=False, if_exists="append"
        )
    except Exception as e:
        db.rollback()
        print("Error writing to database, rolling back transaction.", e)
        # table may not exist yet; create it
        # try:
        #     recipe_ingredient.to_sql(
        #         "recipe_ingredients", db.engine, index=False, if_exists="replace"
        #     )
        # except IntegrityError:
        #     print("Error writing to database: IntegrityError")
        # except Exception as e:
        #     print("Error writing to database:", e)

    recipe_ingredient.to_csv(
        "/home/wandored/Sync/ReportData/recipe_ingredients.csv", index=False
    )

    return recipe_ingredient


def update_ingredient_cost(db) -> None:

    db.execute(
        """
        WITH latestpurchases AS (
         SELECT p.id,
            p.store,
            p.itemid,
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
            lp.itemid,
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
    query = db.fetchall()
    item_cost: pd.DataFrame = pd.DataFrame(
        query,
        columns=[
            "date",
            "id",
            "store",
            "item_id",
            "item",
            "quantity",
            "uofm",
            "amount",
            "base_uofm",
            "base_qty",
            "base_cost",
        ],
    )

    # # get list of unique recipes that begin with "PREP " from recipes table
    # prep_recipes: pd.DataFrame = pd.DataFrame(get_prep_recipes())
    # prep_list = prep_recipes["recipe"].unique().tolist()
    #
    # prep_recipe_costs: pd.DataFrame = pd.DataFrame()
    # for prep in prep_list:
    #     prep_items: pd.DataFrame = recipes[recipes["ingredient"] == prep]
    #     prep_items = prep_items[["ingredient", "qty"]]
    #     prep_items = prep_items.merge(item_cost, left_on="ingredient", right_on="item")
    #     prep_items["amount"] = prep_items["base_cost"] * prep_items["qty"]
    #     prep_cost: pd.DataFrame = (
    #         prep_items.groupby(["id", "store"])["amount"].sum().reset_index()
    #     )
    #     prep_cost["recipe"] = prep
    #     # reorder columns
    #     prep_cost = prep_cost[["recipe", "id", "store", "amount"]]
    #     prep_recipe_costs = pd.concat([prep_recipe_costs, prep_cost])
    #
    # prep_recipe_costs = prep_recipe_costs.merge(prep_recipes, on="recipe", how="left")
    # uofm = get_uofm(db)
    # prep_recipe_costs = prep_recipe_costs.merge(uofm, left_on="uofm", right_on="uofm")
    # prep_recipe_costs["base_cost"] = (
    #     prep_recipe_costs["amount"] / prep_recipe_costs["base_qty"]
    # )
    # prep_recipe_costs["date"] = datetime.now().date()
    # prep_recipe_costs = prep_recipe_costs.rename(columns={"recipe": "item"})
    # prep_recipe_costs = prep_recipe_costs[
    #     [
    #         "date",
    #         "id",
    #         "store",
    #         "item",
    #         "quantity",
    #         "uofm",
    #         "amount",
    #         "base_uofm",
    #         "base_qty",
    #         "base_cost",
    #         "item_id",
    #     ]
    # ]
    #
    # df: pd.DataFrame = pd.concat([prep_recipe_costs, item_cost])
    df = item_cost.copy()
    df = df.rename(columns={"id": "store_id"})
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
            "item_id",
        ]
    ]
    df = df.sort_values(by=["item", "store", "date"])

    # truncate table and re-insert to preserve table structure and constraints
    try:
        db.execute('truncate table "ingredient_cost"')
        db.commit()
        df.to_sql("ingredient_cost", db.engine, index=False, if_exists="append")
    except Exception:
        db.rollback()
        # table may not exist yet; create it
        try:
            df.to_sql("ingredient_cost", db.engine, index=False, if_exists="replace")
        except IntegrityError:
            print("Error writing to database: IntegrityError")
        except Exception as e:
            print("Error writing to database:", e)

    return


def update_recipe_cost(db) -> None:
    db.execute("SELECT locationid, name FROM location")
    location = db.fetchall()
    location: pd.DataFrame = pd.DataFrame(location, columns=["locationid", "name"])

    db.execute("SELECT locationid, name, id FROM restaurants")
    restaurants = db.fetchall()
    restaurants: pd.DataFrame = pd.DataFrame(
        restaurants, columns=["locationid", "name", "id"]
    )
    restaurants = restaurants.rename(columns={"name": "location"})
    restaurants = restaurants.dropna()

    df: pd.DataFrame = pd.merge(restaurants, location, on="locationid", how="left")
    df = df.drop(columns=["locationid"])

    menu_analysis: pd.DataFrame = pd.read_csv(
        "./downloads/Menu Price Analysis.csv",
        skiprows=3,
        sep=",",
        thousands=",",
        usecols=["MenuItemName", "Location", "UnitCost_Loc"],
    )
    menu_analysis = menu_analysis.rename(
        columns={
            "MenuItemName": "menu_item",
            "Location": "name",
            "UnitCost_Loc": "recipe_cost",
        },
    )
    menu_analysis["name"] = menu_analysis["name"].str.strip()
    df = pd.merge(menu_analysis, df, on="name", how="left", sort=False)
    df = df.drop(columns=["name"])
    df[["concept", "menu_item"]] = df["menu_item"].str.split(" - ", n=1, expand=True)
    # set date to yesterday's date
    df["date"] = datetime.now().date() - timedelta(days=1)
    df = df.fillna(0)
    df = df[["date", "id", "location", "concept", "menu_item", "recipe_cost"]]

    # truncate table and re-insert to preserve table structure and constraints
    try:
        db.execute('truncate table "recipe_cost"')
        db.commit()
        df.to_sql("recipe_cost", db.engine, index=False, if_exists="append")
    except Exception:
        db.rollback()
        # table may not exist yet; create it
        try:
            df.to_sql("recipe_cost", db.engine, index=False, if_exists="replace")
        except IntegrityError:
            print("Error writing to database: IntegrityError")
        except Exception as e:
            print("Error writing to database:", e)

    return


# def update_menu_items(db, flat_df) -> None:
#     db.execute("SELECT menu_item_id, concept, menu_item, sales_type FROM menu_items")
#     menu_items = pd.DataFrame(
#         db.fetchall(), columns=["menu_item_id", "concept", "menu_item", "sales_type"]
#     )
#
#     # Extract unique (concept, menu_item) pairs from flat_df
#     new_menu_items = flat_df[["concept", "menu_item"]].drop_duplicates()
#
#     # Remove pairs that already exist in menu_items
#     new_menu_items = new_menu_items.merge(
#         menu_items[["concept", "menu_item"]],
#         on=["concept", "menu_item"],
#         how="left",
#         indicator=True,
#     )
#     new_menu_items = new_menu_items[new_menu_items["_merge"] == "left_only"].drop(
#         columns=["_merge"]
#     )
#
#     if new_menu_items.empty:
#         print("No new menu items to add.")
#         return
#
#     db.execute("SELECT menuitem, sales_type FROM menu_item_sales")
#     menu_item_sales = pd.DataFrame(db.fetchall(), columns=["menu_item", "sales_type"])
#     # Deduplicate sales types to avoid duplicate rows after merge
#     menu_item_sales = menu_item_sales.drop_duplicates(subset=["menu_item"])
#
#     max_id = pd.to_numeric(menu_items["menu_item_id"], errors="coerce").max()
#     max_id = 0 if pd.isna(max_id) else int(max_id)
#
#     new_menu_items = new_menu_items.merge(menu_item_sales, on="menu_item", how="left")
#     # Drop rows where no sales_type could be matched and log them
#     unmatched = new_menu_items[new_menu_items["sales_type"].isna()][
#         ["concept", "menu_item"]
#     ]
#     if not unmatched.empty:
#         print(f"\nDropping {len(unmatched)} menu items with no sales_type match:")
#         print(unmatched.to_string(index=False))
#         unmatched.to_csv("unmatched_menu_items.csv", index=False)
#         print(f"Unmatched items written to unmatched_menu_items.csv")
#     new_menu_items = new_menu_items.dropna(subset=["sales_type"])
#     # Final deduplication after merge to guarantee uniqueness
#     new_menu_items = new_menu_items.drop_duplicates(subset=["concept", "menu_item"])
#     new_menu_items["menu_item_id"] = range(max_id + 1, max_id + 1 + len(new_menu_items))
#     new_menu_items = new_menu_items[
#         ["menu_item_id", "concept", "menu_item", "sales_type"]
#     ]
#     print(f"Adding {len(new_menu_items)} new menu items to the database.")
#     print(new_menu_items)
#     # truncate table and re-insert to preserve table structure and constraints
#     try:
#         new_menu_items.to_sql("menu_items", db.engine, index=False, if_exists="append")
#     except IntegrityError as e:
#         print("Error writing to database, IntegrityError:", e)
#     except Exception as e:
#         print("Error writing to database:", e)
#         raise


if __name__ == "__main__":
    with DatabaseConnection() as db:
        ingredient_update(db)
        update_ingredient_cost(db)
        update_recipe_cost(db)
        # flat_df = ingredient_update_flat(db)
        # update_menu_items(db, flat_df)
        # recreate_all_views(db)
