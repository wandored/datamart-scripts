"""R365 sales details support."""

from datetime import date
from uuid import UUID

import pandas as pd

from src.r365_sync.values import (
    convert_uuid,
    required_boolean,
    required_uuid,
    sales_array,
    sales_numeric,
    unique_r365_records,
)


def r365_sales_detail_rows(tickets):
    """Validate and deduplicate source lines before any aggregation."""
    records = []
    for ticket in tickets:
        for row in sales_array(ticket, "salesDetails"):
            item = row.get("posItem") or {}
            account = row.get("salesAccount") or {}
            records.append(
                {
                    "id": required_uuid(row, "id"),
                    "sales_ticket_id": required_uuid(ticket, "id"),
                    "pos_item_id": item.get("id"),
                    "pos_item_name": item.get("name"),
                    "sale_amount": sales_numeric(row.get("saleAmount")),
                    "quantity": sales_numeric(row.get("quantity")),
                    "void": required_boolean(row, "void"),
                    "sales_account_id": convert_uuid(account.get("id")),
                    "sales_account_name": account.get("name"),
                    "sales_account_number": account.get("number"),
                    "sales_account_gl_type": account.get("glType"),
                    "sales_category": row.get("salesCategory"),
                    "menu_item_category_1": row.get("menuItemCategory1"),
                    "menu_item_category_2": row.get("menuItemCategory2"),
                    "menu_item_category_3": row.get("menuItemCategory3"),
                }
            )
    return pd.DataFrame(
        unique_r365_records(records),
        columns=[
            "id",
            "sales_ticket_id",
            "pos_item_id",
            "pos_item_name",
            "sale_amount",
            "quantity",
            "void",
            "sales_account_id",
            "sales_account_name",
            "sales_account_number",
            "sales_account_gl_type",
            "sales_category",
            "menu_item_category_1",
            "menu_item_category_2",
            "menu_item_category_3",
        ],
        dtype=object,
    )


SALES_DETAIL_GROUP_COLUMNS = (
    "business_date", "location_id", "pos_item_id", "pos_item_name", "void",
    "sales_account_id", "menu_item_category_1", "menu_item_category_2", "menu_item_category_3",
)


def r365_sales_details(detail_rows, business_date, location_id):
    """Sum validated lines at the reporting grain, retaining null-valued groups."""
    groups = {}
    for row in detail_rows.to_dict("records"):
        dimensions = {
            "business_date": date.fromisoformat(str(business_date)),
            "location_id": UUID(str(location_id)),
            **{field: row[field] for field in SALES_DETAIL_GROUP_COLUMNS[2:]},
        }
        key = tuple(dimensions.values())
        if key not in groups:
            groups[key] = {**dimensions, "sale_amount": None, "quantity": None}
        for field in ("sale_amount", "quantity"):
            value = row[field]
            if value is not None:
                previous = groups[key][field]
                groups[key][field] = value if previous is None else previous + value
    return pd.DataFrame(
        list(groups.values()),
        columns=[*SALES_DETAIL_GROUP_COLUMNS, "sale_amount", "quantity"], dtype=object,
    )
