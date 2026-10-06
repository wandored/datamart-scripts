"""R365 vendor invoices support."""

from datetime import datetime

import pandas as pd

from db_utils.dbconnect import DatabaseConnection
from db_utils.r365_importers import get_vendor_invoices
from src.r365_sync.values import convert_uuid, require_vendor_invoice_fields
from src.r365_sync.vendor_invoice_details import r365_vendor_invoice_details
from src.r365_sync.writing import write_to_db


def get_today_vendor_invoices(client):
    """Fetch inventory invoices modified today in the runtime's local timezone."""
    now = datetime.now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone()
    end = now.replace(hour=23, minute=59, second=59, microsecond=999999).astimezone()
    invoices = get_vendor_invoices(
        client,
        modified_on_start=start.isoformat(),
        modified_on_end=end.isoformat(),
    )
    print(f"Fetched {len(invoices):,} vendor invoices modified on {now.date()}.")
    return invoices


def r365_vendor_invoices(client=None, invoices=None):
    if invoices is None:
        invoices = get_today_vendor_invoices(client)
    records = []
    for row in invoices:
        require_vendor_invoice_fields(
            row, ("id", "amount", "date", "documentDate"), "Vendor invoice"
        )
        records.append(
            {
                "id": convert_uuid(row["id"]),
                "transaction_type": row.get("transactionType"),
                "name": row.get("name"),
                "number": row.get("number"),
                "amount": row["amount"],
                "is_paid": row.get("isPaid"),
                "credit_expected": row.get("creditExpected"),
                "comment": row.get("comment"),
                "status": row.get("status"),
                "date": row["date"],
                "document_date": row["documentDate"],
                "due_date": row.get("dueDate"),
                "location_id": convert_uuid((row.get("location") or {}).get("id")),
                "vendor_id": convert_uuid((row.get("vendor") or {}).get("id")),
                "purchase_order_id": convert_uuid(
                    (row.get("purchaseOrder") or {}).get("id")
                ),
            }
        )
    return pd.DataFrame(
        records,
        columns=[
            "id",
            "transaction_type",
            "name",
            "number",
            "amount",
            "is_paid",
            "credit_expected",
            "comment",
            "status",
            "date",
            "document_date",
            "due_date",
            "location_id",
            "vendor_id",
            "purchase_order_id",
        ],
    )


def sync_vendor_invoices(client):
    """Fetch once and commit vendor invoices and their details together."""
    invoices = get_today_vendor_invoices(client)
    invoices_df = r365_vendor_invoices(invoices=invoices)
    details_df = r365_vendor_invoice_details(invoices)
    if invoices_df.empty:
        print("No vendor invoices or vendor invoice details to write.")
        return
    with DatabaseConnection() as db:
        invoice_count = write_to_db(invoices_df, "vendor_invoices", "r365", db=db)
        detail_count = write_to_db(details_df, "vendor_invoice_details", "r365", db=db)
    print(f"Upserted {invoice_count:,} rows into r365.vendor_invoices.")
    print(f"Upserted {detail_count:,} rows into r365.vendor_invoice_details.")


# Uniform entry point used by the command-line dispatcher.
sync = sync_vendor_invoices
