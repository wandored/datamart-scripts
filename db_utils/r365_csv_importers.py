"""CSV adapters for R365 resources without API endpoints.

Return normalized frames separately from database writes so future API adapters
can supply the same columns. Export dates remain text; no timezone is supplied.
"""

import csv
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
