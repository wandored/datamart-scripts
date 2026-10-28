"""R365 sales account support."""

import pandas as pd

from src.r365_sync.values import unique_r365_records


def r365_sales_accounts(detail_rows):
    records = [
        {
            "id": row["sales_account_id"], "name": row["sales_account_name"],
            "number": row["sales_account_number"], "gl_type": row["sales_account_gl_type"],
        }
        for row in detail_rows.to_dict("records") if row["sales_account_id"] is not None
    ]
    return pd.DataFrame(
        unique_r365_records(records), columns=["id", "name", "number", "gl_type"], dtype=object,
    )
