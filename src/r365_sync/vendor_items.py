"""R365 vendor items support."""

from datetime import datetime

import pandas as pd

from db_utils.r365_importers import get_vendor_items
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def r365_vendor_items(client, full_download=False):
    """Fetch all vendor items, or items modified today in the local timezone."""
    if full_download:
        items = get_vendor_items(client)
        scope = "(full download)"
    else:
        now = datetime.now()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone()
        end = now.replace(
            hour=23, minute=59, second=59, microsecond=999999
        ).astimezone()
        items = get_vendor_items(
            client,
            modified_on_start=start.isoformat(),
            modified_on_end=end.isoformat(),
        )
        scope = f"modified on {now.date()}"
    records = []
    for row in items:
        item_id = convert_uuid(row.get("id"))
        if item_id is None:
            raise ValueError("Vendor item is missing its required id")
        if not isinstance(row.get("isPrimary"), bool):
            raise ValueError(f"Vendor item {item_id} requires a boolean isPrimary")
        records.append(
            {
                "id": item_id,
                "name": row.get("name"),
                "vendor_id": convert_uuid((row.get("vendor") or {}).get("id")),
                "number": row.get("number"),
                "brand_item_number": row.get("brandItemNumber"),
                "is_primary": row["isPrimary"],
                "item_id": convert_uuid((row.get("item") or {}).get("id")),
                "purchase_uom_id": convert_uuid(
                    (row.get("purchaseUnitOfMeasure") or {}).get("id")
                ),
                "split_uom_id": convert_uuid(
                    (row.get("splitUnitOfMeasure") or {}).get("id")
                ),
                "vendor_pack_size": row.get("vendorPackSizeDescription"),
                "price": row.get("price"),
                "split_price": row.get("splitPrice"),
                "created_on": row.get("createdOn"),
                "modified_on": row.get("modifiedOn"),
            }
        )
    df = pd.DataFrame(
        records,
        columns=[
            "id",
            "name",
            "vendor_id",
            "number",
            "brand_item_number",
            "is_primary",
            "item_id",
            "purchase_uom_id",
            "split_uom_id",
            "vendor_pack_size",
            "price",
            "split_price",
            "created_on",
            "modified_on",
        ],
    )
    print(f"Fetched {len(df):,} vendor items {scope}.")
    return df


def sync_vendor_items(client, full_download=False):
    items_df = r365_vendor_items(client, full_download=full_download)
    write_to_db(items_df, "vendor_items", "r365")


# Uniform entry point used by the command-line dispatcher.
sync = sync_vendor_items
