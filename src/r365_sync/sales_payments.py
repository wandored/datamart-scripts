"""R365 sales payments support."""

import pandas as pd

from src.r365_sync.values import (
    required_uuid,
    sales_array,
    sales_numeric,
    sales_timestamp,
    unique_r365_records,
)


def r365_sales_payments(tickets, location_timezone=None):
    records = []
    for ticket in tickets:
        for row in sales_array(ticket, "salesPayments"):
            records.append(
                {
                    "id": required_uuid(row, "id"),
                    "sales_ticket_id": required_uuid(ticket, "id"),
                    "amount": sales_numeric(row.get("amount")),
                    "payment_type": row.get("paymentType"),
                    "payment_group": row.get("paymentGroup"),
                    "payment_date": sales_timestamp(
                        row.get("paymentDate"), location_timezone
                    ),
                }
            )
    return pd.DataFrame(
        unique_r365_records(records),
        columns=[
            "id",
            "sales_ticket_id",
            "amount",
            "payment_type",
            "payment_group",
            "payment_date",
        ],
        dtype=object,
    )
