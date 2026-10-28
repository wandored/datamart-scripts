"""R365 employee map support."""

import pandas as pd

from src.r365_sync.values import convert_uuid, required_uuid, unique_r365_records


def r365_employee_map(employees):
    records = [
        {
            "pos_employee_id": required_uuid(row, "posEmployeeId"),
            "employee_id": required_uuid(row, "employeeId"),
        }
        for row in employees
    ]
    return pd.DataFrame(
        unique_r365_records(records, key="pos_employee_id"),
        columns=["pos_employee_id", "employee_id"],
    )
