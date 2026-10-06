"""R365 invoices support."""

from datetime import datetime

import pandas as pd

from db_utils.dbconnect import DatabaseConnection
from db_utils.r365_importers import get_invoices
from src.r365_sync.invoice_details import r365_invoice_details
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def get_today_invoices(client, include_details=False):
    """Fetch invoices modified today, using the runtime's local timezone."""
    now = datetime.now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone()
    end = now.replace(hour=23, minute=59, second=59, microsecond=999999).astimezone()
    invoices = get_invoices(
        client,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        include_details=include_details,
    )
    print(f"Fetched {len(invoices):,} invoices modified on {now.date()}.")
    return invoices


def r365_invoices(client=None, invoices=None):
    """Transform supplied invoices, or fetch today's invoices when omitted."""
    if invoices is None:
        invoices = get_today_invoices(client)

    for row in invoices:
        if convert_uuid(row.get("id")) is None:
            raise ValueError("Invoice is missing its required id")

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "invoice_number": row.get("invoiceNumber"),
                "location_id": convert_uuid((row.get("location") or {}).get("id")),
                "vendor_id": convert_uuid((row.get("vendor") or {}).get("id")),
                "document_date": row.get("documentDate"),
                "document_amount": row.get("documentAmount"),
                "comment": row.get("comment"),
                "status": row.get("status"),
                "payment_terms": row.get("paymentTerms"),
                "due_date": row.get("dueDate"),
                "is_paid": row.get("isPaid"),
                "purchase_order_id": convert_uuid(
                    (row.get("purchaseOrder") or {}).get("id")
                ),
                "created_on": row.get("createdOn"),
                "modified_on": row.get("modifiedOn"),
                "approved_on": row.get("approvedOn"),
            }
            for row in invoices
        ],
        columns=[
            "id",
            "invoice_number",
            "location_id",
            "vendor_id",
            "document_date",
            "document_amount",
            "comment",
            "status",
            "payment_terms",
            "due_date",
            "is_paid",
            "purchase_order_id",
            "created_on",
            "modified_on",
            "approved_on",
        ],
    )
    return df


def sync_invoices(client):
    """Fetch once and commit invoice headers and details together."""
    invoices = get_today_invoices(client, include_details=True)
    invoices_df = r365_invoices(invoices=invoices)
    details_df = r365_invoice_details(invoices)
    if invoices_df.empty:
        print("No invoices or invoice details to write.")
        return

    with DatabaseConnection() as db:
        invoice_count = write_to_db(invoices_df, "invoices", "r365", db=db)
        detail_count = write_to_db(details_df, "invoice_details", "r365", db=db)
    print(f"Upserted {invoice_count:,} rows into r365.invoices.")
    print(f"Upserted {detail_count:,} rows into r365.invoice_details.")


# Uniform entry point used by the command-line dispatcher.
sync = sync_invoices
