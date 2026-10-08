"""CSV adapters for R365 resources that do not yet have API endpoints.

Future API adapters should return the same normalized columns and conflict keys.
Names and units in these exports are labels, not server-provided IDs.
"""

from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import UUID

import pandas as pd


DOWNLOADS = Path(__file__).resolve().parents[1] / "downloads"
RECIPE_COLUMNS = {
    "ID": "id", "Name": "name", "Avg Cost": "avg_cost",
    "Reporting UofM": "reporting_uofm", "Min Cost": "min_cost",
    "Max Cost": "max_cost", "Yield UofM": "yield_uofm", "Yield Qty": "yield_qty",
    "Inventory UofM": "inventory_uofm", "Inventory UofM2": "inventory_uofm2",
    "Inventory UofM3": "inventory_uofm3", "Category 1": "category_1",
    "Category 2": "category_2", "Category 3": "category_3",
    "Cost Account": "cost_account", "Inventory Account": "inventory_account",
    "Waste Account": "waste_account", "Key Item": "key_item", "Number": "number",
    "Description": "description", "Cost Update Method": "cost_update_method",
    "Active": "active", "Available in Inventory": "available_in_inventory",
    "Weight Qty": "weight_qty", "Weight UofM": "weight_uofm",
    "Volume Qty": "volume_qty", "Volume UofM": "volume_uofm",
    "Each Qty": "each_qty", "Each UofM": "each_uofm", "Measure Type": "measure_type",
    "Created By": "created_by", "Created On": "created_on",
    "Modified By": "modified_by", "Modified On": "modified_on",
}
INGREDIENT_COLUMNS = {
    "Recipe": "recipe", "Item": "item", "Qty": "qty", "UofM": "uofm",
    "Yield percent": "yield_percent", "Instructions": "instructions",
    "Created by": "created_by", "Created on": "created_on",
    "Modified by": "modified_by", "Modified on": "modified_on",
}
RECIPE_NUMBERS = (
    "avg_cost", "min_cost", "max_cost", "yield_qty", "weight_qty", "volume_qty", "each_qty",
)
RECIPE_BOOLEANS = ("key_item", "active", "available_in_inventory")
INGREDIENT_KEYS = ("recipe", "item", "uofm")


def _read_csv(path, columns, keys, numbers=(), booleans=()):
    # Keep identifiers, leading zeroes, and literal strings such as "NA" intact.
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    missing = set(columns) - set(frame.columns)
    unexpected = set(frame.columns) - set(columns)
    if missing or unexpected:
        raise ValueError(
            f"{path}: CSV columns changed; missing={sorted(missing)}, "
            f"unexpected={sorted(unexpected)}"
        )
    frame = frame[list(columns)].rename(columns=columns)
    frame = frame.replace(r"^\s*$", None, regex=True)
    for key in keys:
        if frame[key].isna().any():
            raise ValueError(f"{path}: missing required key {key}")
    for column in numbers:
        def decimal_value(value):
            if pd.isna(value):
                return None
            try:
                result = Decimal(value)
                if not result.is_finite():
                    raise InvalidOperation
                return result
            except InvalidOperation:
                raise ValueError(f"{path}: invalid numeric value in {column}") from None
        frame[column] = frame[column].map(decimal_value)
    for column in booleans:
        values = frame[column].str.lower()
        if (frame[column].notna() & ~values.isin(["true", "false"])).any():
            raise ValueError(f"{path}: invalid boolean in {column}")
        frame[column] = values.map({"true": True, "false": False})
    if "id" in keys:
        try:
            frame["id"] = frame["id"].map(UUID)
        except (ValueError, AttributeError):
            raise ValueError(f"{path}: invalid recipe ID") from None
    if frame.duplicated(list(keys)).any():
        raise ValueError(f"{path}: duplicate conflict keys {keys}; import aborted")
    return frame.astype(object).where(pd.notna(frame), None)


def get_recipe_items_csv(path=DOWNLOADS / "RecipeItems.csv"):
    """Return all recipe fields; export timestamps remain unmodified text."""
    return _read_csv(path, RECIPE_COLUMNS, ("id",), RECIPE_NUMBERS, RECIPE_BOOLEANS)


def get_ingredients_csv(path=DOWNLOADS / "ingredients.csv"):
    """Return ingredient lines without inventing source IDs or resolving names."""
    return _read_csv(path, INGREDIENT_COLUMNS, INGREDIENT_KEYS, ("qty", "yield_percent"))
