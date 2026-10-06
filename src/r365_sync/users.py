"""R365 users support."""

import pandas as pd

from db_utils.r365_importers import get_users
from src.r365_sync.values import (
    convert_uuid,
    reference_ids,
    required_boolean,
    required_uuid,
    unique_r365_records,
)
from src.r365_sync.writing import write_to_db


def r365_users(client):
    """The endpoint omits allReportsAccess; leave its database value/default intact."""
    users = get_users(client)
    print(f"Fetched {len(users):,} users.")
    records = [
        {
            "id": required_uuid(row, "id"),
            "default_location_id": convert_uuid(
                (row.get("defaultLocation") or {}).get("id")
            ),
            "inactive": required_boolean(row, "inactive"),
            "all_locations_access": required_boolean(row, "allLocationsAccess"),
            "can_grant_access": required_boolean(
                row, "canGrantAccessBeyondPersonalLevel"
            ),
            "locations_id": reference_ids(row, "locations"),
            "user_rolls_id": reference_ids(row, "userRoles"),
            "report_rolls_id": reference_ids(row, "reportRoles"),
        }
        for row in users
    ]
    return pd.DataFrame(
        unique_r365_records(records),
        columns=[
            "id",
            "default_location_id",
            "inactive",
            "all_locations_access",
            "can_grant_access",
            "locations_id",
            "user_rolls_id",
            "report_rolls_id",
        ],
    )


def sync_users(client):
    write_to_db(r365_users(client), "users", "r365")


# Uniform entry point used by the command-line dispatcher.
sync = sync_users
