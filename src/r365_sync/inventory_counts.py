"""R365 inventory counts support."""

import pandas as pd

from db_utils.r365_importers import get_inventory_counts
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def r365_inventory_counts(client):
    start_date = (pd.Timestamp.now() - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    inventory_counts = get_inventory_counts(
        client,
        business_date_start=start_date,
        business_date_end=end_date,
        include_data="none",
        page_size=250,
    )

    df = pd.DataFrame(
        [
            {
                "id": row["id"],
                "template_name": row["inventoryCountTemplate"]["name"]
                if row["inventoryCountTemplate"]
                else None,
                "location_id": convert_uuid(row["location"]["id"])
                if row["location"]
                else None,
                "name": row["name"],
                "status": row["status"],
                "date": row["date"],
                "frequency": row["frequency"],
                "is_gl_posting": row["isGLPosting"],
                "total_amount": row["totalAmount"],
                "created_on": row["createdOn"],
                "modified_on": row["modifiedOn"],
                "completed_on": row["completedOn"],
                "approved_on": row["approvedOn"],
                "transaction_id": convert_uuid(row["transactionId"]),
                "alerts_count": row["alertsCount"],
            }
            for row in inventory_counts
        ]
    )

    return df


def sync(client):
    frame = r365_inventory_counts(client)
    return write_to_db(frame, "inventory_counts", "r365")
