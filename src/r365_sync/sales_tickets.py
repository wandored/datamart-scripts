"""R365 sales tickets support."""

import pandas as pd

from src.r365_sync.values import (
    convert_uuid,
    required_boolean,
    required_uuid,
    sales_numeric,
    sales_timestamp,
    unique_r365_records,
)


def r365_sales_tickets(tickets, daily_sales_id, location_timezone=None):
    records = []
    for row in tickets:
        records.append(
            {
                "id": required_uuid(row, "id"),
                "daily_sales_id": daily_sales_id,
                "receipt_number": row.get("receiptNumber"),
                "check_number": row.get("checkNumber"),
                "sale_date_time": sales_timestamp(
                    row.get("saleDateTime"), location_timezone
                ),
                "day_of_week": row.get("dayOfWeek"),
                "day_part": row.get("dayPart"),
                "net_sales": sales_numeric(row.get("netSales")),
                "gross_sales": sales_numeric(row.get("grossSales")),
                "sales_amount": sales_numeric(row.get("salesAmount")),
                "guest_count": row.get("guestCount"),
                "order_hour": row.get("orderHour"),
                "tax_amount": sales_numeric(row.get("taxAmount")),
                "tip_amount": sales_numeric(row.get("tipAmount")),
                "total_amount": sales_numeric(row.get("totalAmount")),
                "total_payment": sales_numeric(row.get("totalPayment")),
                "void": required_boolean(row, "void"),
                "server_id": convert_uuid((row.get("server") or {}).get("id")),
            }
        )
    return pd.DataFrame(
        unique_r365_records(records),
        columns=[
            "id",
            "daily_sales_id",
            "receipt_number",
            "check_number",
            "sale_date_time",
            "day_of_week",
            "day_part",
            "net_sales",
            "gross_sales",
            "sales_amount",
            "guest_count",
            "order_hour",
            "tax_amount",
            "tip_amount",
            "total_amount",
            "total_payment",
            "void",
            "server_id",
        ],
        dtype=object,
    )
