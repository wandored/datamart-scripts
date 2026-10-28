"""R365 vendor items support."""

from datetime import datetime

import pandas as pd

from db_utils.r365_importers import get_vendor_items
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def r365_vendor_items(client, full_download=False, **modified_dates):
    """Fetch all vendor items, or items modified today in the local timezone."""
    if modified_dates:
        items = get_vendor_items(client, **modified_dates)
        scope = f"modified from {modified_dates['modified_on_start']} through {modified_dates['modified_on_end']}"
    elif full_download:
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
    records_by_id = {}
    duplicate_count = 0
    for row in items:
        item_id = convert_uuid(row.get("id"))
        if item_id is None:
            raise ValueError("Vendor item is missing its required id")
        if not isinstance(row.get("isPrimary"), bool):
            raise ValueError(f"Vendor item {item_id} requires a boolean isPrimary")
        record = {
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
        if item_id in records_by_id:
            if records_by_id[item_id] != record:
                raise ValueError(f"Conflicting vendor-item records for id {item_id}")
            duplicate_count += 1
            continue
        records_by_id[item_id] = record
    records = list(records_by_id.values())
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
    print(
        f"Fetched {len(items):,} vendor items {scope}; "
        f"collapsed {duplicate_count:,} exact duplicate IDs."
    )
    return df


def sync_vendor_items(client, full_download=False, **modified_dates):
    items_df = r365_vendor_items(client, full_download=full_download, **modified_dates)
    write_to_db(items_df, "vendor_items", "r365")


# Uniform entry point used by the command-line dispatcher.
sync = sync_vendor_items
