"""R365 gl accounts support."""

import pandas as pd

from db_utils.r365_importers import get_glaccounts
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def r365_gl_accounts(client):
    gl_accounts = get_glaccounts(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "number": row["number"],
                "name": row["name"],
                "gl_type": row["glType"],
                "parent_account_id": convert_uuid(row["parentAccount"]["id"])
                if row["parentAccount"]
                else None,
                "operational_report_category": row["operationalReportCategory"],
                "is_stat_account": row["isStatAccount"],
                "disable_entry_subtotal": row["disableEntrySubtotal"],
                "restricted_access": row["restrictedAccess"],
                "control_account": row["controlAccount"],
                "budget_as": row["budgetAs"],
                "percentage_of_based_on": row["percentageOfBasedOn"],
                "budget_as_percentage_of": row["budgetAsPercentageOf"],
                "budget_percentage_or_amount": row["budgetPercentageOrAmount"],
            }
            for row in gl_accounts
        ]
    )

    return df


def sync(client):
    frame = r365_gl_accounts(client)
    return write_to_db(frame, "gl_accounts", "r365")
