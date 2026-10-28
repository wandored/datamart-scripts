"""R365 sales ticket taxes support."""

import pandas as pd

from src.r365_sync.values import required_uuid, sales_array, sales_numeric


def r365_sales_ticket_taxes(tickets):
    records = []
    for ticket in tickets:
        for index, row in enumerate(sales_array(ticket, "taxDetail")):
            records.append(
                {
                    "sales_ticket_id": required_uuid(ticket, "id"),
                    "tax_index": index,
                    "rate_id": row.get("rateId"),
                    "name": row.get("name"),
                    "rate": sales_numeric(row.get("rate")),
                    "tax_type": row.get("taxType"),
                    "inclusion_type": row.get("inclusionType"),
                    "applied_tax_portion_amount": sales_numeric(
                        row.get("appliedTaxPortionAmount")
                    ),
                }
            )
    return pd.DataFrame(
        records,
        columns=[
            "sales_ticket_id",
            "tax_index",
            "rate_id",
            "name",
            "rate",
            "tax_type",
            "inclusion_type",
            "applied_tax_portion_amount",
        ],
        dtype=object,
    )
