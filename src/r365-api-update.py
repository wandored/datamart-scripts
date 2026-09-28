from db_utils.r365_utils import R365Client
from db_utils.r365_importers import (
    get_locations,
    get_units_of_measure,
    get_item_categories,
    get_glaccounts,
    get_purchase_items,
    get_vendors,
    get_inventory_counts,
    get_transactions,
    get_invoices,
    get_vendor_invoices,
    get_vendor_items,
)
from db_utils.dbconnect import DatabaseConnection
from psycopg2 import sql
from datetime import datetime, timedelta
from contextlib import nullcontext
from decimal import Decimal
import argparse
import pandas as pd
from uuid import UUID


def convert_uuid(value):
    if pd.isna(value):
        return None

    if isinstance(value, UUID):
        return value

    return UUID(str(value))


def r365_locations(client):
    locations = get_locations(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "name": row["name"],
                "number": row["number"],
                "timezone": row["timeZone"],
            }
            for row in locations
        ],
        columns=["id", "name", "number", "timezone"],
    )

    return df


def r365_units_of_measure(client):
    units_of_measure = get_units_of_measure(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "name": row["name"],
                "is_base": row["isBase"],
                "is_primitive": row["isPrimitive"],
                "is_purchase": row["isPurchase"],
                "is_recipe": row["isRecipe"],
                "is_active": row["isActive"],
                "base_quantity": row["baseQuantity"],
                "equivalent_quantity": row["equivalentQuantity"],
                "measure_type": row["measureType"],
                "equivalent_uom_id": convert_uuid(row["equivalentUnitOfMeasure"]["id"])
                if row["equivalentUnitOfMeasure"]
                else None,
                "base_uom_id": convert_uuid(row["baseUnitOfMeasure"]["id"])
                if row["baseUnitOfMeasure"]
                else None,
            }
            for row in units_of_measure
        ]
    )

    return df


def r365_item_categories(client):
    item_categories = get_item_categories(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "name": row["name"],
                "item_prefix": row["itemPrefix"],
                "cost_variance_threshold": row["costVarianceThreshold"],
                "counted": row["counted"],
                "actual_as_theoretical": row["actualAsTheoretical"],
                "variance_cap_type": row["varianceCapType"],
                "inventory_variance_threshold": row["inventoryVarianceThreshold"],
                "cost_account_id": convert_uuid(row["costAccount"]["id"])
                if row["costAccount"]
                else None,
                "inventory_account_id": convert_uuid(row["inventoryAccount"]["id"])
                if row["inventoryAccount"]
                else None,
                "waste_account_id": convert_uuid(row["wasteAccount"]["id"])
                if row["wasteAccount"]
                else None,
                "donation_account_id": convert_uuid(row["donationAccount"]["id"])
                if row["donationAccount"]
                else None,
                "type": row["type"],
            }
            for row in item_categories
        ]
    )

    return df


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


def r365_purchase_items(client):
    purchase_items = get_purchase_items(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "name": row["name"],
                "number": row["number"],
                "is_active": row["isActive"],
                "cost_account_id": convert_uuid(row["costAccount"]["id"])
                if row["costAccount"]
                else None,
                "inventory_account_id": convert_uuid(row["inventoryAccount"]["id"])
                if row["inventoryAccount"]
                else None,
                "waste_account_id": convert_uuid(row["wasteAccount"]["id"])
                if row["wasteAccount"]
                else None,
                "donation_account_id": convert_uuid(row["donationAccount"]["id"])
                if row["donationAccount"]
                else None,
                "cost_update_method": row["costUpdateMethod"],
                "description": row["description"],
                "reporting_uom_id": convert_uuid(row["reportingUnitOfMeasure"]["id"])
                if row["reportingUnitOfMeasure"]
                else None,
                "inventory_uom_id": convert_uuid(row["inventoryUnitOfMeasure"]["id"])
                if row["inventoryUnitOfMeasure"]
                else None,
                "inventory_uom_2_id": convert_uuid(row["inventoryUnitOfMeasure2"]["id"])
                if row["inventoryUnitOfMeasure2"]
                else None,
                "inventory_uom_3_id": convert_uuid(row["inventoryUnitOfMeasure3"]["id"])
                if row["inventoryUnitOfMeasure3"]
                else None,
                "equivalence_each_uom_id": convert_uuid(
                    row["equivalenceEachUnitOfMeasure"]["id"]
                )
                if row["equivalenceEachUnitOfMeasure"]
                else None,
                "equivalence_each_quantity": row["equivalenceEachQuantity"],
                "equivalence_volume_uom_id": convert_uuid(
                    row["equivalenceVolumeUnitOfMeasure"]["id"]
                )
                if row["equivalenceVolumeUnitOfMeasure"]
                else None,
                "equivalence_volume_quantity": row["equivalenceVolumeQuantity"],
                "equivalence_weight_uom_id": convert_uuid(
                    row["equivalenceWeightUnitOfMeasure"]["id"]
                )
                if row["equivalenceWeightUnitOfMeasure"]
                else None,
                "equivalence_weight_quantity": row["equivalenceWeightQuantity"],
                "item_category_1_id": convert_uuid(row["itemCategory1"]["id"])
                if row["itemCategory1"]
                else None,
                "item_category_2_id": convert_uuid(row["itemCategory2"]["id"])
                if row["itemCategory2"]
                else None,
                "item_category_3_id": convert_uuid(row["itemCategory3"]["id"])
                if row["itemCategory3"]
                else None,
                "is_key_item": row["isKeyItem"],
                "type": row["type"],
                "measure_type": row["measureType"],
            }
            for row in purchase_items
        ]
    )

    return df


def r365_vendors(client):
    vendors = get_vendors(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "name": row["name"],
                "number": row["number"],
                "comment": row["comment"],
            }
            for row in vendors
        ]
    )

    return df


def r365_inventory_counts(client):
    start_date = (pd.Timestamp.now() - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    inventory_counts = get_inventory_counts(
        client,
        business_date_start=start_date,
        business_date_end=end_date,
        include_data="none",
        page_size=250,
    )

    df = pd.DataFrame(
        [
            {
                "id": row["id"],
                "template_name": row["inventoryCountTemplate"]["name"]
                if row["inventoryCountTemplate"]
                else None,
                "location_id": convert_uuid(row["location"]["id"])
                if row["location"]
                else None,
                "name": row["name"],
                "status": row["status"],
                "date": row["date"],
                "frequency": row["frequency"],
                "is_gl_posting": row["isGLPosting"],
                "total_amount": row["totalAmount"],
                "created_on": row["createdOn"],
                "modified_on": row["modifiedOn"],
                "completed_on": row["completedOn"],
                "approved_on": row["approvedOn"],
                "transaction_id": convert_uuid(row["transactionId"]),
                "alerts_count": row["alertsCount"],
            }
            for row in inventory_counts
        ]
    )

    return df


def r365_transactions(client, location_ids):
    # Preserve the window until R365's modification timezone is confirmed.
    end = datetime.now().date()
    start = end - timedelta(days=1)
    records = []

    for location in location_ids:
        payload = get_transactions(client, location, start_date=start, end_date=end)

        records.extend(
            [
                {
                    "id": convert_uuid(row["id"]),
                    "type": row["type"],
                    "number": row["number"],
                    "date_of_business": row["dateOfBusiness"],
                    "debit_total": row["debitTotal"],
                    "credit_total": row["creditTotal"],
                    "location_id": convert_uuid(row["location"]["id"])
                    if row["location"]
                    else None,
                    "comment": row["comment"],
                    "spread_type": row["spreadType"],
                    "status": row["status"],
                    "modified_on": row["modifiedOn"],
                }
                for row in payload
            ]
        )

    df = pd.DataFrame(
        records,
        columns=[
            "id",
            "type",
            "number",
            "date_of_business",
            "debit_total",
            "credit_total",
            "location_id",
            "comment",
            "spread_type",
            "status",
            "modified_on",
        ],
    )
    return df


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


def require_vendor_invoice_fields(row, fields, record_type):
    for field in fields:
        value = row.get(field)
        if pd.isna(value) or value == "":
            raise ValueError(f"{record_type} is missing required field {field}")


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


def r365_vendor_items(client, full_download=False):
    """Fetch all vendor items, or items modified today in the local timezone."""
    if full_download:
        items = get_vendor_items(client)
        scope = "(full download)"
    else:
        now = datetime.now()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone()
        end = now.replace(
            hour=23, minute=59, second=59, microsecond=999999
        ).astimezone()
        items = get_vendor_items(
            client,
            modified_on_start=start.isoformat(),
            modified_on_end=end.isoformat(),
        )
        scope = f"modified on {now.date()}"
    records = []
    for row in items:
        item_id = convert_uuid(row.get("id"))
        if item_id is None:
            raise ValueError("Vendor item is missing its required id")
        if not isinstance(row.get("isPrimary"), bool):
            raise ValueError(f"Vendor item {item_id} requires a boolean isPrimary")
        records.append(
            {
                "id": item_id,
                "name": row.get("name"),
                "vendor_id": convert_uuid((row.get("vendor") or {}).get("id")),
                "number": row.get("number"),
                "brand_item_number": row.get("brandItemNumber"),
                "is_primary": row["isPrimary"],
                "item_id": convert_uuid((row.get("item") or {}).get("id")),
                "purchase_uom_id": convert_uuid(
                    (row.get("purchaseUnitOfMeasure") or {}).get("id")
                ),
                "split_uom_id": convert_uuid(
                    (row.get("splitUnitOfMeasure") or {}).get("id")
                ),
                "vendor_pack_size": row.get("vendorPackSizeDescription"),
                "price": row.get("price"),
                "split_price": row.get("splitPrice"),
                "created_on": row.get("createdOn"),
                "modified_on": row.get("modifiedOn"),
            }
        )
    df = pd.DataFrame(
        records,
        columns=[
            "id",
            "name",
            "vendor_id",
            "number",
            "brand_item_number",
            "is_primary",
            "item_id",
            "purchase_uom_id",
            "split_uom_id",
            "vendor_pack_size",
            "price",
            "split_price",
            "created_on",
            "modified_on",
        ],
    )
    print(f"Fetched {len(df):,} vendor items {scope}.")
    return df


def sync_vendor_items(client, full_download=False):
    items_df = r365_vendor_items(client, full_download=full_download)
    write_to_db(items_df, "vendor_items", "r365")


def write_to_db(df: pd.DataFrame, table_name: str, schema: str, db=None):
    """Upsert rows; a supplied connection's caller owns commit and reporting."""
    if not schema:
        raise ValueError("schema is required")

    if not table_name:
        raise ValueError("table_name is required")

    if df.empty:
        print(f"No rows to write to {schema}.{table_name}.")
        return 0

    if "id" not in df.columns:
        raise ValueError(f"{schema}.{table_name} must contain an 'id' column")

    owns_connection = db is None
    with DatabaseConnection() if owns_connection else nullcontext(db) as db:
        table_ref = sql.Identifier(schema, table_name)

        columns = [sql.Identifier(col) for col in df.columns]

        update_columns = [col for col in df.columns if col != "id"]

        update_sql = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(col),
                sql.Identifier(col),
            )
            for col in update_columns
        )

        insert_sql = sql.SQL("""
            INSERT INTO {} ({})
            VALUES %s
            ON CONFLICT ({})
            DO UPDATE SET {}
        """).format(
            table_ref,
            sql.SQL(", ").join(columns),
            sql.Identifier("id"),
            update_sql,
        )

        # Convert pandas NaN / NaT / pd.NA to Python None
        df = df.astype(object).where(pd.notna(df), None)

        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

        db.executemany(
            insert_sql.as_string(db.conn),
            rows,
        )

    if owns_connection:
        print(f"Upserted {len(rows):,} rows into {schema}.{table_name}.")
    return len(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-vendor-items",
        action="store_true",
        help="Download and upsert all vendor items only, without date filters.",
    )
    args = parser.parse_args()
    client = R365Client()

    if args.full_vendor_items:
        sync_vendor_items(client, full_download=True)
        raise SystemExit(0)

    locations_df = r365_locations(client)
    location_ids = locations_df["id"].to_list()
    write_to_db(locations_df, "locations", "r365")

    # uofm_df = r365_units_of_measure(client)
    # write_to_db(uofm_df, "units_of_measure", "r365")
    #
    # item_category_df = r365_item_categories(client)
    # write_to_db(item_category_df, "item_categories", "r365")
    #
    # gl_accounts_df = r365_gl_accounts(client)
    # write_to_db(gl_accounts_df, "gl_accounts", "r365")
    #
    # purchase_items_df = r365_purchase_items(client)
    # write_to_db(purchase_items_df, "purchase_items", "r365")
    #
    # vendors_df = r365_vendors(client)
    # write_to_db(vendors_df, "vendors", "r365")

    # inventory_counts_df = r365_inventory_counts(client)
    # write_to_db(inventory_counts_df, "inventory_counts", "r365")

    # transactions_df = r365_transactions(client, location_ids)
    # write_to_db(transactions_df, "transactions", "r365")

    # sync_invoices(client)
    sync_vendor_invoices(client)
    sync_vendor_items(client)
