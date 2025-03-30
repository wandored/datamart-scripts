"""
Identify all item, prep_recipes and menu_recipes not linked to an active menu_item
- items: all items used in prep and/or menu recipes
- prep_recipes: all prep recipes used in menu recipes
- menu_recipes: all menu recipes to be linked to menu items
- ingredients: list of items and prep recipes linked to menu recipes
- menu_items: all active menu items
"""

import pandas as pd
from icecream import ic


def get_menu_item():
    df = pd.read_csv(
        "./downloads/menu_items.csv",
        usecols=["Name", "Recipe"],
    )
    # remove all rows that don't have "Steakhouse" or "Casual"
    df = df[df["Name"].str.contains("Steakhouse") | df["Name"].str.contains("Casual")]
    df["Name"] = df["Name"].str.replace("Steakhouse - ", "")
    df["Name"] = df["Name"].str.replace("Casual - ", "")
    df.rename(columns={"Name": "menu_item", "Recipe": "recipe_name"}, inplace=True)
    return df


def get_ingredients():
    df = pd.read_csv(
        "./downloads/ingredients.csv",
        usecols=["Item", "Recipe"],
    )
    df.rename(
        columns={
            "Item": "ingredient",
            "Recipe": "recipe_name",
        },
        inplace=True,
    )
    return df


def get_menu_recipes():
    df = pd.read_csv("./downloads/menu_recipes.csv", usecols=["RecipeName", "Active"])
    df.rename(columns={"RecipeName": "recipe_name", "Active": "active"}, inplace=True)
    return df


def get_prep_recipes():
    df = pd.read_csv("./downloads/prep_recipes.csv", usecols=["RecipeName", "Active"])
    df.rename(columns={"RecipeName": "recipe_name", "Active": "active"}, inplace=True)
    return df


def get_purchase_items():
    df = pd.read_csv("./downloads/items.csv", usecols=["itemName", "Active"])
    df.rename(columns={"itemName": "purchase_item", "Active": "active"}, inplace=True)
    return df


if __name__ == "__main__":
    menu_items = get_menu_item()
    ingredients = get_ingredients()
    menu_recipes = get_menu_recipes()
    prep_recipes = get_prep_recipes()
    purchase_items = get_purchase_items()

    # get all unique recipe_name in menu_items
    linked_recipe_names = menu_items["recipe_name"].unique()
    linked_ingredients = pd.concat(
        [ingredients["ingredient"], purchase_items["purchase_item"]]
    ).unique()

    # get all menu recipes not linked to a menu item
    orphan_menu_recipes = menu_recipes[
        ~menu_recipes["recipe_name"].isin(linked_recipe_names)
    ]
    # write to csv
    orphan_menu_recipes.to_csv("./output/orphan_menu_recipes.csv", index=False)
    # get all prep recipes linked to orphan menu recipes
    deadend_prep_recipes = prep_recipes[
        prep_recipes["recipe_name"].isin(orphan_menu_recipes["recipe_name"])
    ]
    deadend_prep_recipes.to_csv("./output/deadend_prep_recipes.csv", index=False)

    # get all prep recipes not used in menu recipes
    orphan_prep_recipes = prep_recipes[
        ~prep_recipes["recipe_name"].isin(ingredients["recipe_name"])
    ]
    orphan_prep_recipes.to_csv("./output/orphan_prep_recipes.csv", index=False)

    # get all purchase items linked to orphan menu recipes
    deadend_purchase_items = purchase_items[
        purchase_items["purchase_item"].isin(orphan_menu_recipes["recipe_name"])
    ]
    deadend_purchase_items.to_csv("./output/deadend_purchase_items.csv", index=False)

    # get all purchase items not used in menu prep and/or menu recipes
    orphan_purchase_items = purchase_items[
        ~purchase_items["purchase_item"].isin(ingredients["ingredient"])
    ]
    orphan_purchase_items.to_csv("./output/orphan_purchase_items.csv", index=False)

    ic(orphan_menu_recipes)
    ic(orphan_prep_recipes)
    ic(deadend_prep_recipes)
    ic(orphan_purchase_items)
    ic(deadend_purchase_items)
