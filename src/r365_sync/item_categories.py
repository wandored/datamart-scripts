"""R365 item categories support."""

import pandas as pd

from db_utils.r365_importers import get_item_categories
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def r365_item_categories(client):
    item_categories = get_item_categories(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "name": row["name"],
                "item_prefix": row["itemPrefix"],
                "cost_variance_threshold": row["costVarianceThreshold"],
                "counted": row["counted"],
                "actual_as_theoretical": row["actualAsTheoretical"],
                "variance_cap_type": row["varianceCapType"],
                "inventory_variance_threshold": row["inventoryVarianceThreshold"],
                "cost_account_id": convert_uuid(row["costAccount"]["id"])
                if row["costAccount"]
                else None,
                "inventory_account_id": convert_uuid(row["inventoryAccount"]["id"])
                if row["inventoryAccount"]
                else None,
                "waste_account_id": convert_uuid(row["wasteAccount"]["id"])
                if row["wasteAccount"]
                else None,
                "donation_account_id": convert_uuid(row["donationAccount"]["id"])
                if row["donationAccount"]
                else None,
                "type": row["type"],
            }
            for row in item_categories
        ]
    )

    return df


def sync(client):
    frame = r365_item_categories(client)
    return write_to_db(frame, "item_categories", "r365")
