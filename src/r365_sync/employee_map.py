"""R365 employee map support."""

import pandas as pd

from src.r365_sync.values import convert_uuid, required_uuid, unique_r365_records


def r365_employee_map(employees):
    records = []
    for row in employees:
        pos_employee_id = convert_uuid(row.get("posEmployeeId"))
        if pos_employee_id is not None:
            records.append(
                {
                    "pos_employee_id": pos_employee_id,
                    "employee_id": required_uuid(row, "employeeId"),
                }
            )
    return pd.DataFrame(
        unique_r365_records(records, key="pos_employee_id"),
        columns=["pos_employee_id", "employee_id"],
    )
