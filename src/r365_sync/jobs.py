"""R365 jobs support."""

import pandas as pd

from db_utils.r365_importers import get_jobs
from src.r365_sync.dates import labor_date_range, labor_options
from src.r365_sync.values import (
    convert_uuid,
    required_boolean,
    required_uuid,
    unique_r365_records,
)
from src.r365_sync.writing import write_to_db


def r365_jobs(client, modified_on_start=None, modified_on_end=None):
    start, end = labor_date_range(modified_on_start, modified_on_end)
    jobs = get_jobs(client, start, end)
    print(f"Fetched {len(jobs):,} jobs modified from {start} through {end}.")
    records = [
        {
            "id": required_uuid(row, "id"),
            "name": row.get("name"),
            "code": row.get("code"),
            "department": row.get("department"),
            "pay_rate": row.get("payRate"),
            "exclude_from_schedule": required_boolean(row, "excludeFromSchedule"),
            "exclude_from_pos_import": required_boolean(row, "excludeFromPOSImport"),
            "location_id": convert_uuid((row.get("location") or {}).get("id")),
            "general_ledger_account_id": convert_uuid(
                (row.get("generalLedgerAccount") or {}).get("id")
            ),
        }
        for row in jobs
    ]
    return pd.DataFrame(
        unique_r365_records(records),
        columns=[
            "id",
            "name",
            "code",
            "department",
            "pay_rate",
            "exclude_from_schedule",
            "exclude_from_pos_import",
            "location_id",
            "general_ledger_account_id",
        ],
    )


def sync_jobs(client, modified_on_start=None, modified_on_end=None):
    jobs_df = r365_jobs(client, modified_on_start, modified_on_end)
    write_to_db(jobs_df, "jobs", "r365")


# Uniform entry point used by the command-line dispatcher.
sync = sync_jobs


def get_options(args):
    return labor_options(args)
