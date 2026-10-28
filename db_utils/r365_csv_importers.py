"""CSV adapters for R365 resources without API endpoints.

Return normalized frames separately from database writes so future API adapters
can supply the same columns. Export dates remain text; no timezone is supplied.
"""

import csv
from decimal import Decimal, InvalidOperation
import re
from pathlib import Path
from uuid import UUID

import pandas as pd


DOWNLOADS = Path(__file__).resolve().parents[1] / "downloads"
POS_ITEM_COLUMNS = {
    "Id": "id", "PosItemId": "pos_item_id", "POS ID": "pos_id",
    "SKU": "sku", "Barcode": "barcode", "POS Item Name": "pos_item_name",
    "PLU": "plu", "Type": "type", "Menu Item": "menu_item",
    "Created on": "created_on", "Modified on": "modified_on",
    "is Menu Item": "is_menu_item", "Modified by": "modified_by",
    "Created by": "created_by",
}
MENU_ITEM_COLUMNS = {
    "Id": "id", "Recipe Id": "recipe_id", "Is Recipe Assigned": "is_recipe_assigned",
    "Name": "name", "Type": "type", "Recipe": "recipe", "Category 1": "category_1",
    "Category 2": "category_2", "Category 3": "category_3",
    "Created on": "created_on", "Modified on": "modified_on",
}


def _read_csv(path, columns, *, uuid_columns, boolean_columns, integer_columns=(),
              required=("id",), unique=("id",)):
    records = []
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        headers = reader.fieldnames or []
        if len(headers) != len(columns) or set(headers) != set(columns):
            raise ValueError(f"{path}: CSV headers do not match the expected export columns")
        for line, source in enumerate(reader, start=2):
            if None in source or any(value is None for value in source.values()):
                raise ValueError(f"{path}: malformed CSV row {line}")
            row = {target: source[key] if source[key].strip() else None
                   for key, target in columns.items()}
            for column in required:
                if row[column] is None:
                    raise ValueError(f"{path}: missing {column} at row {line}")
            for column in uuid_columns:
                if row[column] is not None:
                    try:
                        row[column] = UUID(row[column])
                    except ValueError:
                        raise ValueError(f"{path}: invalid UUID in {column} at row {line}") from None
            for column in integer_columns:
                value = row[column]
                if value is not None:
                    if not re.fullmatch(r"[0-9]+", value) or int(value) > 2**63 - 1:
                        raise ValueError(f"{path}: invalid bigint in {column} at row {line}")
                    row[column] = int(value)
            for column in boolean_columns:
                value = row[column]
                if value is not None:
                    mapping = {"yes": True, "no": False, "true": True, "false": False}
                    if value.lower() not in mapping:
                        raise ValueError(f"{path}: invalid boolean in {column} at row {line}")
                    row[column] = mapping[value.lower()]
            records.append(row)
    # Object dtype preserves integer IDs without rounding through float64.
    frame = pd.DataFrame(records, columns=list(columns.values()), dtype=object)
    for column in unique:
        if frame[column].duplicated().any():
            raise ValueError(f"{path}: duplicate {column}; import aborted")
    return frame


def get_pos_items_csv(path=DOWNLOADS / "PosItems.csv"):
    return _read_csv(
        path, POS_ITEM_COLUMNS, uuid_columns=("id",), boolean_columns=("is_menu_item",),
        integer_columns=("pos_item_id",), required=("id", "pos_item_id"),
        unique=("id", "pos_item_id"),
    )


def get_menu_items_csv(path=DOWNLOADS / "MenuItems.csv"):
    return _read_csv(
        path, MENU_ITEM_COLUMNS, uuid_columns=("id", "recipe_id"),
        boolean_columns=("is_recipe_assigned",),
    )


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


def _read_recipe_csv(path, columns, keys, numbers=(), booleans=()):
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
    return _read_recipe_csv(path, RECIPE_COLUMNS, ("id",), RECIPE_NUMBERS, RECIPE_BOOLEANS)


def get_ingredients_csv(path=DOWNLOADS / "ingredients.csv"):
    """Return ingredient lines without inventing source IDs or resolving names."""
    return _read_recipe_csv(path, INGREDIENT_COLUMNS, INGREDIENT_KEYS, ("qty", "yield_percent"))
