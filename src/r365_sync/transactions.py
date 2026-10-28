"""R365 transactions support."""

from datetime import datetime, timedelta

import pandas as pd

from db_utils.r365_importers import get_transactions
from src.r365_sync.locations import get_location_ids
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def r365_transactions(client, location_ids, business_date_start=None, business_date_end=None,
                      modified_on_start=None, modified_on_end=None):
    # Preserve the window until R365's modification timezone is confirmed.
    end = datetime.now().date()
    start = end - timedelta(days=1)
    records = []

    for location in location_ids:
        if modified_on_start is not None or modified_on_end is not None:
            payload = get_transactions(client, location, start_date=modified_on_start, end_date=modified_on_end)
        elif business_date_start is None and business_date_end is None:
            payload = get_transactions(client, location, start_date=start, end_date=end)
        else:
            payload = get_transactions(
                client, location, business_date_start=business_date_start,
                business_date_end=business_date_end,
            )

        records.extend(
            [
                {
                    "id": convert_uuid(row["id"]),
                    "type": row["type"],
                    "number": row["number"],
                    "date_of_business": row["dateOfBusiness"],
                    "debit_total": row["debitTotal"],
                    "credit_total": row["creditTotal"],
                    "location_id": convert_uuid(row["location"]["id"])
                    if row["location"]
                    else None,
                    "comment": row["comment"],
                    "spread_type": row["spreadType"],
                    "status": row["status"],
                    "modified_on": row["modifiedOn"],
                }
                for row in payload
            ]
        )

    df = pd.DataFrame(
        records,
        columns=[
            "id",
            "type",
            "number",
            "date_of_business",
            "debit_total",
            "credit_total",
            "location_id",
            "comment",
            "spread_type",
            "status",
            "modified_on",
        ],
    )
    return df


def sync(client):
    frame = r365_transactions(client, get_location_ids(client))
    return write_to_db(frame, "transactions", "r365")
