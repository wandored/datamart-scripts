"""R365 vendor invoice details support."""

import pandas as pd

from src.r365_sync.values import convert_uuid, require_vendor_invoice_fields


def r365_vendor_invoice_details(invoices):
    records = []
    for invoice in invoices:
        require_vendor_invoice_fields(invoice, ("id",), "Vendor invoice")
        invoice_id = convert_uuid(invoice["id"])
        for detail in invoice.get("details") or []:
            require_vendor_invoice_fields(
                detail,
                ("id", "eachAmount", "quantity", "total"),
                f"Detail for vendor invoice {invoice_id}",
            )
            records.append(
                {
                    "id": convert_uuid(detail["id"]),
                    "vendor_invoice_id": invoice_id,
                    "each_amount": detail["eachAmount"],
                    "general_ledger_account_id": convert_uuid(
                        (detail.get("generalLedgerAccount") or {}).get("id")
                    ),
                    "item_id": convert_uuid((detail.get("item") or {}).get("id")),
                    "quantity": detail["quantity"],
                    "quantity_ordered": detail.get("quantityOrdered"),
                    "total": detail["total"],
                    "unit_of_measure_id": convert_uuid(
                        (detail.get("unitOfMeasure") or {}).get("id")
                    ),
                    "vendor_item_id": convert_uuid(
                        (detail.get("vendorItem") or {}).get("id")
                    ),
                }
            )
    df = pd.DataFrame(
        records,
        columns=[
            "id",
            "vendor_invoice_id",
            "each_amount",
            "general_ledger_account_id",
            "item_id",
            "quantity",
            "quantity_ordered",
            "total",
            "unit_of_measure_id",
            "vendor_item_id",
        ],
    )
    print(f"Fetched {len(df):,} vendor invoice details.")
    return df
