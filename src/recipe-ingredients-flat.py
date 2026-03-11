import pandas as pd
from collections import defaultdict
from db_utils.dbconnect import DatabaseConnection
from sqlalchemy.exc import IntegrityError


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


def get_menu_recipe() -> pd.DataFrame:
    df = pd.read_csv("./downloads/MenuItems_R365.csv")
    # drop all rows where Category 1 is null
    df = df.dropna(subset=["Category 1"])
    # keep only Name and Recipe columns
    df = df[["Id", "Name", "Recipe"]]
    # split Name column into two columns
    df[["concept", "menu_item"]] = df["Name"].str.split(" - ", expand=True)
    df = df.rename(columns={"Id": "menu_item_id", "Recipe": "recipe"})
    return df


def get_prep_recipes() -> pd.DataFrame:
    df = pd.read_csv("./downloads/RecipeItems.csv")
    # drop all rows where Name does not start with "PREP "
    df = df[df["Name"].str.startswith("PREP ")]
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


def ingredient_update_flat() -> pd.DataFrame:
    with DatabaseConnection() as db:
        recipe_ingredient = get_recipe_ingredient()
        uofm = get_uofm(db)
        menu_recipe = get_menu_recipe()
        prep_recipe_items = get_prep_recipes()

        # Normalize names same way as recipe column
        prep_recipe_items["recipe"] = prep_recipe_items["recipe"].str.strip()
        prep_recipe_names = set(
            prep_recipe_items["recipe"].unique()
        )  # for quick lookup during recursion

        # Merge with UOFM to convert yield to base
        prep_recipe_items = prep_recipe_items.merge(
            uofm,
            left_on="uofm",
            right_on="uofm",
            how="left",
        )

        prep_recipe_items["yield_base_qty"] = (
            prep_recipe_items["quantity"] * prep_recipe_items["base_qty"]
        )

        recipe_yield_map = dict(
            zip(prep_recipe_items["recipe"], prep_recipe_items["yield_base_qty"])
        )

        # ---------------------------------------------------------
        # 1️⃣ Convert all recipe quantities to base UOFM
        # ---------------------------------------------------------

        recipe_ingredient = recipe_ingredient.merge(uofm, on="uofm", how="left")
        recipe_ingredient["qty"] = (
            recipe_ingredient["qty"] * recipe_ingredient["base_qty"]
        )

        recipe_ingredient = recipe_ingredient.drop(columns=["uofm", "base_qty"])
        recipe_ingredient = recipe_ingredient.rename(columns={"base_uofm": "uofm"})

        # ---------------------------------------------------------
        # 2 Build recipe graph for recursion
        # ---------------------------------------------------------

        recipe_map = defaultdict(list)

        for _, row in recipe_ingredient.iterrows():
            recipe_map[row["recipe"]].append(
                {
                    "ingredient": row["ingredient"],
                    "qty": row["qty"],
                    "uofm": row["uofm"],
                }
            )

        # ---------------------------------------------------------
        # 4️⃣ Recursive PREP explosion
        # ---------------------------------------------------------

        def explode_recipe(recipe_name, multiplier=1.0, visited=None):
            if visited is None:
                visited = set()

            if recipe_name in visited:
                raise ValueError(f"Circular recipe detected: {recipe_name}")

            visited.add(recipe_name)

            components = recipe_map.get(recipe_name, [])
            results = []

            # Get yield (default = 1 if missing)
            recipe_yield = recipe_yield_map.get(recipe_name, 1.0)

            for comp in components:
                ingredient = comp["ingredient"]
                qty = comp["qty"]
                uofm = comp["uofm"]

                # 🔹 Adjust for yield
                effective_multiplier = multiplier * (qty / recipe_yield)

                if ingredient in prep_recipe_names:
                    results.extend(
                        explode_recipe(
                            ingredient,
                            multiplier=effective_multiplier,
                            visited=visited.copy(),
                        )
                    )
                else:
                    results.append(
                        {
                            "ingredient": ingredient,
                            "qty": effective_multiplier,
                            "uofm": uofm,
                        }
                    )

            return results

        # ---------------------------------------------------------
        # 3 Attach concept + primary menu item
        # ---------------------------------------------------------

        recipe_ingredient = recipe_ingredient.merge(
            menu_recipe, on="recipe", how="left"
        )

        recipe_ingredient = recipe_ingredient[recipe_ingredient["concept"].notna()]

        # ---------------------------------------------------------
        # 5️⃣ Explode only MENU recipes
        # ---------------------------------------------------------

        flattened_rows = []

        menu_recipes = recipe_ingredient[
            ~recipe_ingredient["recipe"].str.startswith("PREP ")
        ][["menu_item_id", "concept", "menu_item", "recipe"]].drop_duplicates()

        for _, row in menu_recipes.iterrows():
            menu_item_id = row["menu_item_id"]
            concept = row["concept"]
            menu_item = row["menu_item"]
            recipe_name = row["recipe"]

            exploded = explode_recipe(recipe_name)

            for comp in exploded:
                flattened_rows.append(
                    {
                        "menu_item_id": menu_item_id,
                        "concept": concept,
                        "menu_item": menu_item,
                        "recipe": recipe_name,
                        "ingredient": comp["ingredient"],
                        "qty": comp["qty"],
                        "uofm": comp["uofm"],
                    }
                )

        flat_df = pd.DataFrame(flattened_rows)

        # create bridge table of menu_item_id and recipe_id
        db.execute(
            """
            SELECT recipe_id, recipe_name
            FROM recipes
            """
        )
        recipes = db.fetchall()
        recipes_df = pd.DataFrame(recipes, columns=["recipe_id", "recipe_name"])
        menu_item_recipes = flat_df[["menu_item_id", "recipe"]].drop_duplicates()
        menu_item_recipes = menu_item_recipes.merge(
            recipes_df,
            left_on="recipe",
            right_on="recipe_name",
            how="left",
        )[["menu_item_id", "recipe_id"]]
        try:
            db.execute('truncate table "menu_item_recipes"')
            db.commit()
            menu_item_recipes.to_sql(
                "menu_item_recipes",
                db.engine,
                index=False,
                if_exists="append",
            )
        except Exception:
            db.rollback()
            try:
                menu_item_recipes.to_sql(
                    "menu_item_recipes",
                    db.engine,
                    index=False,
                    if_exists="replace",
                )
            except Exception as e:
                print("Error writing to database:", e)

        # ---------------------------------------------------------
        # 6️⃣ Aggregate duplicate ingredients
        # ---------------------------------------------------------

        flat_df = flat_df.groupby(
            ["concept", "menu_item", "recipe", "ingredient", "uofm", "menu_item_id"],
            as_index=False,
        ).sum()

        flat_df = flat_df.sort_values(by=["menu_item", "concept", "ingredient"])

        # add recipe_id column by merging with recipes table.
        db.execute(
            """
            SELECT recipe_id, recipe_name
            FROM recipes
            """
        )
        recipes = db.fetchall()
        recipes_df = pd.DataFrame(recipes, columns=["recipe_id", "recipe_name"])
        flat_df = flat_df.merge(
            recipes_df,
            left_on="recipe",
            right_on="recipe_name",
            how="left",
        )
        # flat_df = flat_df.drop(columns=["recipe_name", "recipe"])

        # add item_id column by merging with item table
        db.execute(
            """
            SELECT itemid, name
            FROM item
            """
        )
        items = db.fetchall()
        items_df = pd.DataFrame(items, columns=["item_id", "ingredient"])
        flat_df = flat_df.merge(
            items_df,
            on="ingredient",
            how="left",
        )
        # flat_df = flat_df.drop(columns=["ingredient"])

        # ---------------------------------------------------------
        # 7️⃣ Write to database
        # ---------------------------------------------------------
        # reorder columns
        flat_df = flat_df[
            [
                "menu_item_id",
                "recipe_id",
                "item_id",
                "qty",
                "uofm",
            ]
        ]
        print(flat_df.head(25))

        try:
            db.execute('truncate table "recipe_ingredients_flat"')
            db.commit()
            flat_df.to_sql(
                "recipe_ingredients_flat",
                db.engine,
                index=False,
                if_exists="append",
            )
        except Exception:
            db.rollback()
            try:
                flat_df.to_sql(
                    "recipe_ingredients_flat",
                    db.engine,
                    index=False,
                    if_exists="replace",
                )
            except Exception as e:
                print("Error writing to database:", e)

        return


if __name__ == "__main__":
    ingredient_update_flat()
