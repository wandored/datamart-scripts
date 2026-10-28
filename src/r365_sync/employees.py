"""R365 employees support."""

from uuid import UUID

import pandas as pd

from db_utils.dbconnect import DatabaseConnection
from db_utils.r365_importers import get_employees
from src.r365_sync.dates import labor_date_range, labor_options
from src.r365_sync.employee_map import r365_employee_map
from src.r365_sync.locations import get_location_ids
from src.r365_sync.values import (
    convert_uuid,
    reference_ids,
    required_boolean,
    required_uuid,
    unique_r365_records,
)
from src.r365_sync.writing import write_to_db


def r365_employees(employees):
    records = [
        {
            "id": required_uuid(row, "employeeId"),
            "pay_rate": row.get("payRate"),
            "pay_schedule": row.get("paySchedule"),
            "hire_date": row.get("hireDate"),
            "termination_date": row.get("terminationDate"),
            "payroll_id": row.get("payrollId"),
            "primary_location_id": convert_uuid(
                (row.get("primaryLocation") or {}).get("id")
            ),
            "primary_job_id": convert_uuid((row.get("primaryJob") or {}).get("id")),
            "inactive": required_boolean(row, "inactive"),
            "other_locations_id": reference_ids(row, "otherLocations"),
            "other_jobs_id": reference_ids(row, "otherJobs"),
        }
        for row in employees
    ]
    return pd.DataFrame(
        unique_r365_records(records),
        columns=[
            "id",
            "pay_rate",
            "pay_schedule",
            "hire_date",
            "termination_date",
            "payroll_id",
            "primary_location_id",
            "primary_job_id",
            "inactive",
            "other_locations_id",
            "other_jobs_id",
        ],
    )


def sync_employees(client, location_ids, modified_on_start=None, modified_on_end=None):
    start, end = labor_date_range(modified_on_start, modified_on_end)
    if not location_ids:
        print("No locations available; skipping employee sync.")
        return
    employees = get_employees(client, location_ids, start, end)
    print(f"Fetched {len(employees):,} employees modified from {start} through {end}.")
    employees = filter_employees_with_pos_uuid(employees)
    employees_df = r365_employees(employees)
    mapping_df = r365_employee_map(employees)
    if employees_df.empty:
        print("No employees or employee mappings to write.")
        return
    with DatabaseConnection() as db:
        employee_count = write_to_db(employees_df, "employees", "r365", db=db)
        if not mapping_df.empty:
            db.executemany(
                """
                INSERT INTO r365.employee_map (pos_employee_id, employee_id)
                VALUES %s
                ON CONFLICT (pos_employee_id) DO UPDATE
                SET employee_id = EXCLUDED.employee_id
                """,
                list(mapping_df.itertuples(index=False, name=None)),
            )
    print(f"Upserted {employee_count:,} rows into r365.employees.")
    print(f"Upserted {len(mapping_df):,} rows into r365.employee_map.")


def sync(client, modified_on_start=None, modified_on_end=None):
    return sync_employees(
        client, get_location_ids(client), modified_on_start, modified_on_end,
    )


def get_options(args):
    return labor_options(args)


def filter_employees_with_pos_uuid(employees):
    """Exclude archived POS records before transforming employees or mappings."""
    retained = []
    missing_pos = 0
    non_uuid_pos = 0
    for row in employees:
        value = row.get("posEmployeeId")
        if pd.isna(value) or (isinstance(value, str) and not value.strip()):
            missing_pos += 1
            continue
        try:
            convert_uuid(value)
        except ValueError:
            non_uuid_pos += 1
            continue
        retained.append(row)
    print(
        f"Retained {len(retained):,} employee records with UUID POS IDs; "
        f"skipped {missing_pos:,} with missing/blank POS IDs and "
        f"{non_uuid_pos:,} with non-UUID POS IDs."
    )
    return retained
