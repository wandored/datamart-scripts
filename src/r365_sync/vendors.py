"""R365 vendors support."""

import pandas as pd

from db_utils.r365_importers import get_vendors
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def r365_vendors(client):
    vendors = get_vendors(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "name": row["name"],
                "number": row["number"],
                "comment": row["comment"],
            }
            for row in vendors
        ]
    )

    return df


def sync(client):
    frame = r365_vendors(client)
    return write_to_db(frame, "vendors", "r365")
