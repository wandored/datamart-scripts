"""R365 invoice details support."""

from decimal import Decimal

import pandas as pd

from src.r365_sync.values import convert_uuid


def r365_invoice_details(invoices):
    """Flatten details from fetched invoices, retaining each parent invoice id."""
    records = []
    for invoice in invoices:
        invoice_id = convert_uuid(invoice.get("id"))
        if invoice_id is None:
            raise ValueError("Invoice is missing its required id")
        for detail in invoice.get("details") or []:
            detail_id = convert_uuid(detail.get("id"))
            if detail_id is None:
                raise ValueError(f"Invoice {invoice_id} has a detail without an id")
            record = {
                "id": detail_id,
                "invoice_id": invoice_id,
                "gl_account_id": convert_uuid(
                    (detail.get("glAccount") or {}).get("id")
                ),
                "location_id": convert_uuid((detail.get("location") or {}).get("id")),
                "comment": detail.get("comment"),
                "total": detail.get("total"),
                "inventory_item_id": convert_uuid(
                    (detail.get("inventoryItem") or {}).get("id")
                ),
                "uom_id": convert_uuid((detail.get("uom") or {}).get("id")),
                "quantity": detail.get("quantity"),
                "each_amount": detail.get("eachAmount"),
                "vendor_item_number": detail.get("vendorItemNumber"),
            }
            for column in ("total", "quantity", "each_amount"):
                if record[column] is not None:
                    record[column] = Decimal(str(record[column]))
            records.append(record)

    df = pd.DataFrame(
        records,
        columns=[
            "id",
            "invoice_id",
            "gl_account_id",
            "location_id",
            "comment",
            "total",
            "inventory_item_id",
            "uom_id",
            "quantity",
            "each_amount",
            "vendor_item_number",
        ],
    )
    print(f"Fetched {len(df):,} invoice details.")
    return df
