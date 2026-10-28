"""R365 vendor invoice details support."""

import pandas as pd

from src.r365_sync.values import convert_uuid, require_vendor_invoice_fields


def r365_vendor_invoice_details(invoices):
    records_by_id = {}
    duplicate_count = 0
    for invoice in invoices:
        require_vendor_invoice_fields(invoice, ("id",), "Vendor invoice")
        invoice_id = convert_uuid(invoice["id"])
        for detail in invoice.get("details") or []:
            require_vendor_invoice_fields(
                detail,
                ("id", "eachAmount", "quantity", "total"),
                f"Detail for vendor invoice {invoice_id}",
            )
            detail_id = convert_uuid(detail["id"])
            record = {
                "id": detail_id,
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
            if detail_id in records_by_id:
                if records_by_id[detail_id] != record:
                    raise ValueError("Conflicting vendor invoice detail records for duplicate id")
                duplicate_count += 1
                continue
            records_by_id[detail_id] = record
    records = list(records_by_id.values())
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
    print(
        f"Fetched {len(df) + duplicate_count:,} vendor invoice details; "
        f"collapsed {duplicate_count:,} exact duplicate IDs."
    )
    return df
