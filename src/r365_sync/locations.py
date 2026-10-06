"""R365 locations support."""

import pandas as pd

from db_utils.r365_importers import get_locations
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def r365_locations(client):
    locations = get_locations(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "name": row["name"],
                "number": row["number"],
                "timezone": row["timeZone"],
            }
            for row in locations
        ],
        columns=["id", "name", "number", "timezone"],
    )

    return df


def sync(client):
    frame = r365_locations(client)
    return write_to_db(frame, "locations", "r365")


def get_location_ids(client):
    return [row["id"] for row in get_locations(client)]
