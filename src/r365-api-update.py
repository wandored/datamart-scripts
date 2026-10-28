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
    get_employees,
    get_jobs,
    get_users,
    get_daily_sales_pages,
    get_labor_punches,
    get_payroll_summaries,
    get_sales_analytics,
)
from db_utils.dbconnect import DatabaseConnection
from db_utils.r365_csv_importers import (
    get_pos_items_csv, get_menu_items_csv, get_recipe_items_csv, get_ingredients_csv,
    INGREDIENT_KEYS,
)
from db_utils import fiscal_calendar
from psycopg2 import sql
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from contextlib import nullcontext
from decimal import Decimal
import argparse
import sys
import pandas as pd
from uuid import UUID


def convert_uuid(value):
    if pd.isna(value):
        return None

    if isinstance(value, UUID):
        return value

    return UUID(str(value))


def r365_locations(client, **modified_dates):
    locations = get_locations(client, **modified_dates)

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


def r365_units_of_measure(client, **modified_dates):
    units_of_measure = get_units_of_measure(client, **modified_dates)

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


def r365_item_categories(client, **modified_dates):
    item_categories = get_item_categories(client, **modified_dates)

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


def r365_gl_accounts(client, **modified_dates):
    gl_accounts = get_glaccounts(client, **modified_dates)

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


def r365_purchase_items(client, **modified_dates):
    purchase_items = get_purchase_items(client, **modified_dates)

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


def r365_vendors(client, **modified_dates):
    vendors = get_vendors(client, **modified_dates)

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


def r365_inventory_counts(client, start_date=None, end_date=None, **modified_dates):
    if start_date is None and end_date is None and not modified_dates:
        start_date = (pd.Timestamp.now() - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    inventory_counts = get_inventory_counts(
        client,
        business_date_start=start_date,
        business_date_end=end_date,
        include_data="none",
        page_size=250,
        **modified_dates,
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


def r365_transactions(client, location_ids, business_date_start=None, business_date_end=None,
                      modified_on_start=None, modified_on_end=None):
    # Preserve the window until R365's modification timezone is confirmed.
    end = datetime.now().date()
    start = end - timedelta(days=1)
    records = []

    for location in location_ids:
        if modified_on_start is not None or modified_on_end is not None:
            payload = get_transactions(client, location, start_date=modified_on_start, end_date=modified_on_end)
        elif business_date_start is None and business_date_end is None:
            payload = get_transactions(client, location, start_date=start, end_date=end)
        else:
            payload = get_transactions(
                client, location, business_date_start=business_date_start,
                business_date_end=business_date_end,
            )

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


def sync_invoices(client, document_date_start=None, document_date_end=None):
    """Fetch once and commit invoice headers and details together."""
    if document_date_start is None and document_date_end is None:
        invoices = get_today_invoices(client, include_details=True)
    else:
        invoices = get_invoices(
            client, include_details=True, document_date_start=document_date_start,
            document_date_end=document_date_end,
        )
        print(f"Fetched {len(invoices):,} invoices.")
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
    records_by_id = {}
    duplicate_count = 0
    for row in invoices:
        require_vendor_invoice_fields(
            row, ("id", "amount", "date", "documentDate"), "Vendor invoice"
        )
        invoice_id = convert_uuid(row["id"])
        record = {
            "id": invoice_id,
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
        if invoice_id in records_by_id:
            if records_by_id[invoice_id] != record:
                raise ValueError(f"Conflicting vendor invoice records for id {invoice_id}")
            duplicate_count += 1
            continue
        records_by_id[invoice_id] = record
    print(
        f"Fetched {len(records_by_id) + duplicate_count:,} vendor invoices; "
        f"collapsed {duplicate_count:,} exact duplicate IDs."
    )
    return pd.DataFrame(
        list(records_by_id.values()),
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


def sync_vendor_invoices(client, date_start=None, date_end=None, **modified_dates):
    """Fetch once and commit vendor invoices and their details together."""
    if modified_dates:
        invoices = get_vendor_invoices(client, **modified_dates)
    elif date_start is None and date_end is None:
        invoices = get_today_vendor_invoices(client)
    else:
        invoices = get_vendor_invoices(client, date_start=date_start, date_end=date_end)
        print(f"Fetched {len(invoices):,} vendor invoices.")
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


def r365_vendor_items(client, full_download=False, **modified_dates):
    """Fetch all vendor items, or items modified today in the local timezone."""
    if modified_dates:
        items = get_vendor_items(client, **modified_dates)
        scope = f"modified from {modified_dates['modified_on_start']} through {modified_dates['modified_on_end']}"
    elif full_download:
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
    records_by_id = {}
    duplicate_count = 0
    for row in items:
        item_id = convert_uuid(row.get("id"))
        if item_id is None:
            raise ValueError("Vendor item is missing its required id")
        if not isinstance(row.get("isPrimary"), bool):
            raise ValueError(f"Vendor item {item_id} requires a boolean isPrimary")
        record = {
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
        if item_id in records_by_id:
            if records_by_id[item_id] != record:
                raise ValueError(f"Conflicting vendor-item records for id {item_id}")
            duplicate_count += 1
            continue
        records_by_id[item_id] = record
    records = list(records_by_id.values())
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
    print(
        f"Fetched {len(items):,} vendor items {scope}; "
        f"collapsed {duplicate_count:,} exact duplicate IDs."
    )
    return df


def sync_vendor_items(client, full_download=False, **modified_dates):
    items_df = r365_vendor_items(client, full_download=full_download, **modified_dates)
    write_to_db(items_df, "vendor_items", "r365")


def required_uuid(row, field):
    value = convert_uuid(row.get(field))
    if value is None:
        raise ValueError(f"R365 record requires {field}")
    return value


def required_boolean(row, field):
    value = row.get(field)
    if not isinstance(value, bool):
        raise ValueError(f"R365 record requires a boolean {field}")
    return value


def reference_ids(row, field):
    """Preserve null versus empty relationship arrays and validate every ID."""
    references = row.get(field)
    if references is None:
        return None
    if not isinstance(references, list):
        raise ValueError(f"R365 {field} must be a list")
    return [required_uuid(reference, "id") for reference in references]


def unique_r365_records(records, key="id"):
    """Collapse identical records; do not silently choose conflicting values."""
    unique = {}
    for record in records:
        record_id = record[key]
        if record_id in unique and unique[record_id] != record:
            raise ValueError(f"Conflicting R365 records for {key} {record_id}")
        unique[record_id] = record
    return list(unique.values())


def labor_date_range(modified_on_start=None, modified_on_end=None):
    """Labor filters accept dates only; one supplied boundary means that day."""
    start = modified_on_start or modified_on_end or datetime.now().date().isoformat()
    end = modified_on_end or start
    start = date.fromisoformat(str(start))
    end = date.fromisoformat(str(end))
    if start > end:
        raise ValueError("Modified-on start must not be after modified-on end")
    return start.isoformat(), end.isoformat()


def r365_employees(employees):
    records = [
        {
            "id": required_uuid(row, "employeeId"),
            "pay_rate": row.get("payRate"),
            "pay_schedule": row.get("paySchedule"),
            "hire_date": row.get("hireDate"),
            "termination_date": row.get("terminationDate"),
            "payroll_id": row.get("payrollId"),
            "primary_location_id": convert_uuid(
                (row.get("primaryLocation") or {}).get("id")
            ),
            "primary_job_id": convert_uuid((row.get("primaryJob") or {}).get("id")),
            "inactive": required_boolean(row, "inactive"),
            "other_locations_id": reference_ids(row, "otherLocations"),
            "other_jobs_id": reference_ids(row, "otherJobs"),
        }
        for row in employees
    ]
    return pd.DataFrame(
        unique_r365_records(records),
        columns=[
            "id",
            "pay_rate",
            "pay_schedule",
            "hire_date",
            "termination_date",
            "payroll_id",
            "primary_location_id",
            "primary_job_id",
            "inactive",
            "other_locations_id",
            "other_jobs_id",
        ],
    )


def filter_employees_with_pos_uuid(employees):
    """Exclude archived POS records before transforming employees or mappings."""
    retained = []
    missing_pos = 0
    non_uuid_pos = 0
    for row in employees:
        value = row.get("posEmployeeId")
        if pd.isna(value) or (isinstance(value, str) and not value.strip()):
            missing_pos += 1
            continue
        try:
            convert_uuid(value)
        except ValueError:
            non_uuid_pos += 1
            continue
        retained.append(row)
    print(
        f"Retained {len(retained):,} employee records with UUID POS IDs; "
        f"skipped {missing_pos:,} with missing/blank POS IDs and "
        f"{non_uuid_pos:,} with non-UUID POS IDs."
    )
    return retained


def r365_employee_map(employees):
    records = [
        {
            "pos_employee_id": required_uuid(row, "posEmployeeId"),
            "employee_id": required_uuid(row, "employeeId"),
        }
        for row in employees
    ]
    return pd.DataFrame(
        unique_r365_records(records, key="pos_employee_id"),
        columns=["pos_employee_id", "employee_id"],
    )


def sync_employees(client, location_ids, modified_on_start=None, modified_on_end=None):
    start, end = labor_date_range(modified_on_start, modified_on_end)
    if not location_ids:
        print("No locations available; skipping employee sync.")
        return
    employees = get_employees(client, location_ids, start, end)
    print(f"Fetched {len(employees):,} employees modified from {start} through {end}.")
    employees = filter_employees_with_pos_uuid(employees)
    employees_df = r365_employees(employees)
    mapping_df = r365_employee_map(employees)
    if employees_df.empty:
        print("No employees or employee mappings to write.")
        return
    with DatabaseConnection() as db:
        employee_count = write_to_db(employees_df, "employees", "r365", db=db)
        if not mapping_df.empty:
            db.executemany(
                """
                INSERT INTO r365.employee_map (pos_employee_id, employee_id)
                VALUES %s
                ON CONFLICT (pos_employee_id) DO UPDATE
                SET employee_id = EXCLUDED.employee_id
                """,
                list(mapping_df.itertuples(index=False, name=None)),
            )
    print(f"Upserted {employee_count:,} rows into r365.employees.")
    print(f"Upserted {len(mapping_df):,} rows into r365.employee_map.")


def r365_jobs(client, modified_on_start=None, modified_on_end=None):
    start, end = labor_date_range(modified_on_start, modified_on_end)
    jobs = get_jobs(client, start, end)
    print(f"Fetched {len(jobs):,} jobs modified from {start} through {end}.")
    records = [
        {
            "id": required_uuid(row, "id"),
            "name": row.get("name"),
            "code": row.get("code"),
            "department": row.get("department"),
            "pay_rate": row.get("payRate"),
            "exclude_from_schedule": required_boolean(row, "excludeFromSchedule"),
            "exclude_from_pos_import": required_boolean(row, "excludeFromPOSImport"),
            "location_id": convert_uuid((row.get("location") or {}).get("id")),
            "general_ledger_account_id": convert_uuid(
                (row.get("generalLedgerAccount") or {}).get("id")
            ),
        }
        for row in jobs
    ]
    return pd.DataFrame(
        unique_r365_records(records),
        columns=[
            "id",
            "name",
            "code",
            "department",
            "pay_rate",
            "exclude_from_schedule",
            "exclude_from_pos_import",
            "location_id",
            "general_ledger_account_id",
        ],
    )


def sync_jobs(client, modified_on_start=None, modified_on_end=None):
    jobs_df = r365_jobs(client, modified_on_start, modified_on_end)
    write_to_db(jobs_df, "jobs", "r365")


def r365_users(client, **modified_dates):
    """The endpoint omits allReportsAccess; leave its database value/default intact."""
    users = get_users(client, **modified_dates)
    print(f"Fetched {len(users):,} users.")
    records = [
        {
            "id": required_uuid(row, "id"),
            "default_location_id": convert_uuid(
                (row.get("defaultLocation") or {}).get("id")
            ),
            "inactive": required_boolean(row, "inactive"),
            "all_locations_access": required_boolean(row, "allLocationsAccess"),
            "can_grant_access": required_boolean(
                row, "canGrantAccessBeyondPersonalLevel"
            ),
            "locations_id": reference_ids(row, "locations"),
            "user_rolls_id": reference_ids(row, "userRoles"),
            "report_rolls_id": reference_ids(row, "reportRoles"),
        }
        for row in users
    ]
    return pd.DataFrame(
        unique_r365_records(records),
        columns=[
            "id",
            "default_location_id",
            "inactive",
            "all_locations_access",
            "can_grant_access",
            "locations_id",
            "user_rolls_id",
            "report_rolls_id",
        ],
    )


def sync_users(client, **modified_dates):
    write_to_db(r365_users(client, **modified_dates), "users", "r365")



LABOR_PUNCH_COLUMNS = (
    "labor_id", "date_worked", "start_time", "end_time", "hours", "pay_rate", "total",
    "tip_declared_amount", "employee_id", "job_id", "location_id", "created_on",
    "created_by_id", "modified_on", "modified_by_id",
)
# R365 may omit earningType even when Regular Pay and Charge Tips are separate rows.
PAYROLL_KEYS = ("business_date", "location_id", "legal_entity_id", "employee_id", "job_code_id", "earning_type", "earning_type_name")
PAYROLL_COLUMNS = (*PAYROLL_KEYS, "total_seconds", "rate", "cost")
ANALYTICS_KEYS = ("location_id", "business_date", "start_date_time")
ANALYTICS_MEASURES = {
    "forecasted_net_sales": "ForecastedNetSales", "forecasted_gross_sales": "ForecastedGrossSales",
    "forecasted_entree": "ForecastedEntree", "forecasted_labor": "ForecastedLabor",
    "actual_net_sales": "ActualNetSales", "actual_gross_sales": "ActualGrossSales",
    "actual_entree_count": "ActualEntreeCount", "actual_labor": "ActualLabor",
    "actual_labor_ot_hours": "ActualLaborOTHours", "actual_labor_dollars": "ActualLaborDollars",
    "optimal_labor_variance": "OptimalLaborVariance", "optimal_labor": "OptimalLabor",
}
ANALYTICS_JOB_MEASURES = {
    "labor_forecast": "LaborForecast", "actual_labor": "ActualLabor",
    "actual_labor_ot_hours": "ActualLaborOTHours", "actual_labor_dollars": "ActualLaborDollars",
}


def labor_reference(row, field):
    return convert_uuid((row.get(field) or {}).get("id"))


def labor_frame(records, columns, keys):
    """Keep explicit empty schemas; reject conflicting rows at the source grain."""
    unique = {}
    for record in records:
        key = tuple(record[column] for column in keys)
        if key in unique and unique[key] != record:
            raise ValueError("Conflicting labor records for the same key")
        unique[key] = record
    return pd.DataFrame(list(unique.values()), columns=columns, dtype=object)


def r365_labor_punches(rows, location_timezones):
    records = []
    for row in rows:
        location_id = labor_reference(row, "location")
        zone = location_timezones.get(str(location_id))
        record = {
            "labor_id": required_uuid(row, "laborId"),
            "date_worked": date.fromisoformat(row["dateWorked"]),
            "employee_id": labor_reference(row, "employee"),
            "job_id": labor_reference(row, "job"), "location_id": location_id,
            "created_by_id": labor_reference(row, "createdBy"),
            "modified_by_id": labor_reference(row, "modifiedBy"),
        }
        for column, field in (("start_time", "startTime"), ("end_time", "endTime"),
                              ("created_on", "createdOn"), ("modified_on", "modifiedOn")):
            record[column] = sales_timestamp(row.get(field), zone)
        for column, field in (("hours", "hours"), ("pay_rate", "payRate"),
                              ("total", "total"), ("tip_declared_amount", "tipDeclaredAmount")):
            record[column] = sales_numeric(row.get(field))
        records.append(record)
    return labor_frame(records, LABOR_PUNCH_COLUMNS, ("labor_id",))


def r365_payroll_summaries(rows, location_id, start, end):
    records = []
    for row in rows:
        day = date.fromisoformat(row["businessDate"])
        actual_location = required_uuid(row.get("location") or {}, "id")
        if actual_location != UUID(str(location_id)) or not start <= day <= end:
            raise ValueError("Payroll summary does not match requested location/date")
        records.append({
            "business_date": day, "location_id": actual_location,
            "legal_entity_id": labor_reference(row, "legalEntity"),
            "employee_id": required_uuid(row.get("employee") or {}, "id"),
            "job_code_id": labor_reference(row, "jobCode"),
            "earning_type": row.get("earningType"), "earning_type_name": row.get("earningTypeName"),
            "total_seconds": row.get("totalSeconds"),
            "rate": sales_numeric(row.get("rate")), "cost": sales_numeric(row.get("cost")),
        })
    return labor_frame(records, PAYROLL_COLUMNS, PAYROLL_KEYS)


def analytics_fields(record):
    """Accept live camelCase and documented PascalCase without losing measures."""
    if not isinstance(record, dict):
        raise ValueError("Sales analytics records must be objects")
    normalized = {}
    for key, value in record.items():
        field = key[:1].lower() + key[1:]
        if field in normalized and normalized[field] != value:
            raise ValueError(f"Conflicting sales analytics field aliases: {field}")
        normalized[field] = value
    return normalized


def r365_sales_analytics(response, location_id, start, end, location_timezone):
    response = analytics_fields(response)
    if required_uuid(response, "locationId") != UUID(str(location_id)):
        raise ValueError("Sales analytics does not match requested location")
    intervals, jobs = [], []
    for daily in sales_array(response, "dailyData"):
        daily = analytics_fields(daily)
        day = datetime.fromisoformat(daily["date"]).date()
        if not start <= day <= end:
            raise ValueError("Sales analytics does not match requested dates")
        for interval in sales_array(daily, "intervals"):
            interval = analytics_fields(interval)
            stamp = sales_timestamp(interval["startDateTime"], location_timezone)
            if stamp is None:
                raise ValueError("Sales analytics requires an interval timestamp")
            key = {"location_id": UUID(str(location_id)), "business_date": day, "start_date_time": stamp}
            intervals.append({
                **key, "published_at": sales_timestamp(interval.get("publishedAt"), location_timezone),
                **{column: sales_numeric(interval.get(field[:1].lower() + field[1:])) for column, field in ANALYTICS_MEASURES.items()},
            })
            for job in sales_array(interval, "jobDetails"):
                job = analytics_fields(job)
                jobs.append({**key, "job_id": required_uuid(job, "jobId"),
                             **{column: sales_numeric(job.get(field[:1].lower() + field[1:])) for column, field in ANALYTICS_JOB_MEASURES.items()}})
    return (
        labor_frame(intervals, (*ANALYTICS_KEYS, "published_at", *ANALYTICS_MEASURES), ANALYTICS_KEYS),
        labor_frame(jobs, (*ANALYTICS_KEYS, "job_id", *ANALYTICS_JOB_MEASURES), (*ANALYTICS_KEYS, "job_id")),
    )


def sync_dated_labor(client, start, end, tables, modified_dates=None):
    """Upsert punches; reconcile complete payroll/forecast business-date snapshots."""
    locations = get_locations(client)
    timezones = {str(row["id"]): row.get("timeZone") for row in locations}
    if "labor_punches" in tables:
        filters = modified_dates or {"business_date_start": start, "business_date_end": end}
        rows = get_labor_punches(client, **filters)
        frame = r365_labor_punches(rows, timezones)
        if not modified_dates and any(not start <= day <= end for day in frame["date_worked"]):
            raise ValueError("Labor punches do not match requested business dates")
        frame["last_synced_at"] = datetime.now(timezone.utc)
        count = write_to_db(frame, "labor_punches", "r365", conflict_columns=("labor_id",))
        print(f"Labor punches fetched={len(rows):,}, upserted={count:,}.")
    for location_id, zone in timezones.items():
        if "payroll_summaries" in tables:
            rows = get_payroll_summaries(client, location_id, start, end)
            frame = r365_payroll_summaries(rows, location_id, start, end)
            count = write_labor_snapshot("payroll_summaries", frame, PAYROLL_KEYS, location_id, start, end)
            print(f"Payroll summaries {location_id} {start} through {end}: fetched/upserted={count:,}.")
        if "sales_analytics" in tables:
            response = get_sales_analytics(client, location_id, start, end)
            intervals, jobs = r365_sales_analytics(response, location_id, start, end, zone)
            with DatabaseConnection() as db:
                interval_count = write_labor_snapshot("sales_analytics", intervals, ANALYTICS_KEYS, location_id, start, end, db=db)
                job_count = write_labor_snapshot("sales_analytics_jobs", jobs, (*ANALYTICS_KEYS, "job_id"), location_id, start, end, db=db)

            print(f"Sales analytics {location_id} {start} through {end}: intervals={interval_count:,}, jobs={job_count:,}.")


def write_labor_snapshot(table, frame, keys, location_id, start, end, db=None):
    """Mark missing aggregate rows inactive only after a complete validated response."""
    frame["last_synced_at"] = datetime.now(timezone.utc)
    frame["is_current"] = True
    with DatabaseConnection() if db is None else nullcontext(db) as connection:
        connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"r365.labor:{location_id}",),
        )
        connection.execute(sql.SQL("""
            UPDATE r365.{} SET is_current = false
            WHERE location_id = %s AND business_date BETWEEN %s AND %s AND is_current
        """).format(sql.Identifier(table)), (UUID(str(location_id)), start, end))
        count = write_to_db(frame, table, "r365", db=connection, conflict_columns=keys)
    return count


def daily_sales_date_range(business_date_start=None, business_date_end=None):
    """Default to the previous seven completed dates; one boundary means one day."""
    if business_date_start is None and business_date_end is None:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=6)
    else:
        start = date.fromisoformat(str(business_date_start or business_date_end))
        end = date.fromisoformat(str(business_date_end or business_date_start))
    if start > end:
        raise ValueError("Business-date start must not be after business-date end")
    return start, end


def sales_array(record, field):
    """Missing arrays indicate an incomplete payload; explicit null means no rows."""
    if field not in record:
        raise ValueError(f"Daily sales record is missing {field}")
    value = record[field]
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise ValueError(f"Daily sales {field} must be an array of objects or null")
    return value


def sales_numeric(value):
    if value is None:
        return None
    value = Decimal(str(value))
    if not value.is_finite():
        raise ValueError("Daily sales numeric values must be finite")
    return value


# Common US Windows timezone IDs returned by R365, using Unicode CLDR's
# territory="001" mappings. IANA names continue to pass through unchanged.
# https://github.com/unicode-org/cldr/blob/main/common/supplemental/windowsZones.xml
R365_WINDOWS_TIMEZONES = {
    "Eastern Standard Time": "America/New_York",
    "Central Standard Time": "America/Chicago",
    "Mountain Standard Time": "America/Denver",
    "US Mountain Standard Time": "America/Phoenix",
    "Pacific Standard Time": "America/Los_Angeles",
    "Alaskan Standard Time": "America/Anchorage",
    "Hawaiian Standard Time": "Pacific/Honolulu",
    "UTC": "Etc/UTC",
}


def sales_timestamp(value, location_timezone=None):
    if value is None:
        return None
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        if not location_timezone:
            raise ValueError("Offset-free sales timestamps require a location timezone")
        zone = ZoneInfo(R365_WINDOWS_TIMEZONES.get(location_timezone, location_timezone))
        early = stamp.replace(tzinfo=zone, fold=0)
        late = stamp.replace(tzinfo=zone, fold=1)
        if early.utcoffset() != late.utcoffset():
            raise ValueError(
                "Ambiguous or nonexistent local sales timestamp needs an offset"
            )
        stamp = early
    return stamp.astimezone(timezone.utc)


def r365_daily_sales(pages, location_id, business_date):
    columns = [
        "id",
        "location_id",
        "business_date",
        "net_sales",
        "gross_sales",
        "guest_count",
        "total_labor_hours",
        "total_labor_amount",
        "total_labor_percentage",
        "total_deposit",
        "expected_cash",
        "paid_in_total",
        "paid_out_total",
        "over_short",
    ]
    records = []
    for row in pages:
        record = {
            "id": required_uuid(row, "id"),
            "location_id": required_uuid(row.get("location") or {}, "id"),
            "business_date": date.fromisoformat(row["businessDate"]),
            "net_sales": sales_numeric(row.get("netSales")),
            "gross_sales": sales_numeric(row.get("grossSales")),
            "guest_count": row.get("guestCount"),
            "total_labor_hours": sales_numeric(row.get("totalLaborHours")),
            "total_labor_amount": sales_numeric(row.get("totalLaborAmount")),
            "total_labor_percentage": sales_numeric(row.get("totalLaborPercentage")),
            "total_deposit": sales_numeric(row.get("totalDeposit")),
            "expected_cash": sales_numeric(row.get("expectedCash")),
            "paid_in_total": sales_numeric(row.get("paidInTotal")),
            "paid_out_total": sales_numeric(row.get("paidOutTotal")),
            "over_short": sales_numeric(row.get("overShort")),
        }
        if record["location_id"] != UUID(str(location_id)) or record[
            "business_date"
        ] != date.fromisoformat(str(business_date)):
            raise ValueError(
                "Daily sales response does not match requested location/date"
            )
        records.append(record)
    records = unique_r365_records(records)
    if len(records) != 1:
        raise ValueError(
            "Daily sales pages must contain exactly one consistent summary"
        )
    return pd.DataFrame(records, columns=columns, dtype=object)


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


def r365_sales_detail_rows(tickets):
    """Validate and deduplicate source lines before any aggregation."""
    records = []
    for ticket in tickets:
        for row in sales_array(ticket, "salesDetails"):
            item = row.get("posItem") or {}
            account = row.get("salesAccount") or {}
            records.append(
                {
                    "id": required_uuid(row, "id"),
                    "sales_ticket_id": required_uuid(ticket, "id"),
                    "pos_item_id": item.get("id"),
                    "pos_item_name": item.get("name"),
                    "sale_amount": sales_numeric(row.get("saleAmount")),
                    "quantity": sales_numeric(row.get("quantity")),
                    "void": required_boolean(row, "void"),
                    "sales_account_id": convert_uuid(account.get("id")),
                    "sales_account_name": account.get("name"),
                    "sales_account_number": account.get("number"),
                    "sales_account_gl_type": account.get("glType"),
                    "sales_category": row.get("salesCategory"),
                    "menu_item_category_1": row.get("menuItemCategory1"),
                    "menu_item_category_2": row.get("menuItemCategory2"),
                    "menu_item_category_3": row.get("menuItemCategory3"),
                }
            )
    return pd.DataFrame(
        unique_r365_records(records),
        columns=[
            "id",
            "sales_ticket_id",
            "pos_item_id",
            "pos_item_name",
            "sale_amount",
            "quantity",
            "void",
            "sales_account_id",
            "sales_account_name",
            "sales_account_number",
            "sales_account_gl_type",
            "sales_category",
            "menu_item_category_1",
            "menu_item_category_2",
            "menu_item_category_3",
        ],
        dtype=object,
    )


SALES_DETAIL_GROUP_COLUMNS = (
    "business_date", "location_id", "pos_item_id", "pos_item_name", "void",
    "sales_account_id", "menu_item_category_1", "menu_item_category_2", "menu_item_category_3",
)


def r365_sales_details(detail_rows, business_date, location_id):
    """Sum validated lines at the reporting grain, retaining null-valued groups."""
    groups = {}
    for row in detail_rows.to_dict("records"):
        dimensions = {
            "business_date": date.fromisoformat(str(business_date)),
            "location_id": UUID(str(location_id)),
            **{field: row[field] for field in SALES_DETAIL_GROUP_COLUMNS[2:]},
        }
        key = tuple(dimensions.values())
        if key not in groups:
            groups[key] = {**dimensions, "sale_amount": None, "quantity": None}
        for field in ("sale_amount", "quantity"):
            value = row[field]
            if value is not None:
                previous = groups[key][field]
                groups[key][field] = value if previous is None else previous + value
    return pd.DataFrame(
        list(groups.values()),
        columns=[*SALES_DETAIL_GROUP_COLUMNS, "sale_amount", "quantity"], dtype=object,
    )


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


def get_active_sales_locations():
    """Use the restaurant list, excluding inactive rows and unmapped entities."""
    with DatabaseConnection() as db:
        db.execute("""
            SELECT r365_guid AS id, timezone
            FROM core.restaurants
            WHERE active IS TRUE AND r365_guid IS NOT NULL
            ORDER BY r365_guid
        """)
        return db.fetchall()


def sync_daily_sales(
    client,
    location_ids=None,
    business_date_start=None,
    business_date_end=None,
    location_timezones=None,
):
    start, end = daily_sales_date_range(business_date_start, business_date_end)
    if location_ids is None:
        locations = get_active_sales_locations()
        location_ids = [row["id"] for row in locations]
        location_timezones = {str(row["id"]): row["timezone"] for row in locations}
    if not location_ids:
        print("No active restaurants with an R365 GUID; skipping daily sales.")
        return
    for location_id in dict.fromkeys(location_ids):
        business_date = start
        while business_date <= end:
            pages = get_daily_sales_pages(client, business_date, location_id)
            if not pages:
                print(
                    f"Skipped daily sales for {location_id} on {business_date}: "
                    "API returned 404 (missing summary or location); stored rows unchanged."
                )
                business_date += timedelta(days=1)
                continue
            summary = r365_daily_sales(pages, location_id, business_date)
            tickets = [
                ticket for page in pages for ticket in sales_array(page, "salesTickets")
            ]
            # Deduplicate complete tickets before flattening positional taxes. A changed
            # ticket across pages makes the snapshot unsafe to reconcile.
            for ticket in tickets:
                required_uuid(ticket, "id")
            tickets = unique_r365_records(tickets)
            summary_id = summary.iloc[0]["id"]
            location_timezone = (location_timezones or {}).get(str(location_id))
            detail_rows = r365_sales_detail_rows(tickets)
            frames = {
                "daily_sales": summary,
                "sales_tickets": r365_sales_tickets(
                    tickets, summary_id, location_timezone
                ),
                "sales_account": r365_sales_accounts(detail_rows),
                "sales_details": r365_sales_details(detail_rows, business_date, location_id),
                "sales_payments": r365_sales_payments(tickets, location_timezone),
                "sales_ticket_taxes": r365_sales_ticket_taxes(tickets),
            }
            synced_at = datetime.now(timezone.utc)
            for table, frame in frames.items():
                frame["last_synced_at"] = synced_at
                if table not in ("daily_sales", "sales_account"):
                    frame["is_current"] = True
            counts = {}
            with DatabaseConnection() as db:
                replaced_id = remove_superseded_daily_sales(
                    db, UUID(str(location_id)), business_date, summary_id,
                )
                counts["daily_sales"] = write_to_db(
                    summary, "daily_sales", "r365", db=db,
                )
                db.execute(
                    """
                    UPDATE r365.sales_details SET is_current = false
                    WHERE location_id = %s AND business_date = %s AND is_current
                    """,
                    (UUID(str(location_id)), business_date),
                )
                for table in ("sales_payments", "sales_ticket_taxes"):
                    db.execute(
                        sql.SQL("""
                        UPDATE r365.{} AS child SET is_current = false
                        FROM r365.sales_tickets AS ticket
                        WHERE child.sales_ticket_id = ticket.id
                          AND ticket.daily_sales_id = %s AND child.is_current
                    """).format(sql.Identifier(table)),
                        (summary_id,),
                    )
                db.execute(
                    """
                    UPDATE r365.sales_tickets SET is_current = false
                    WHERE daily_sales_id = %s AND is_current
                """,
                    (summary_id,),
                )
                for table, frame in frames.items():
                    if table == "daily_sales":
                        continue
                    if table == "sales_ticket_taxes":
                        keys = ("sales_ticket_id", "tax_index")
                    elif table == "sales_details":
                        keys = SALES_DETAIL_GROUP_COLUMNS
                    else:
                        keys = ("id",)
                    counts[table] = write_to_db(
                        frame, table, "r365", db=db, conflict_columns=keys
                    )
            if replaced_id is not None:
                print(
                    f"Daily sales {location_id} {business_date}: replaced summary "
                    f"{replaced_id} and its dependent rows with {summary_id}."
                )
            print(
                f"Daily sales {location_id} {business_date} ({len(detail_rows):,} source item lines): "
                + ", ".join(
                    f"{table} fetched/upserted={count:,}"
                    for table, count in counts.items()
                )
            )
            business_date += timedelta(days=1)


def remove_superseded_daily_sales(db, location_id, business_date, summary_id):
    """Remove a replaced summary's owned rows inside the caller's transaction."""
    # Serialize imports even when the location/day has no header yet. A row lock
    # alone cannot protect concurrent first inserts or a delete/insert replacement.
    db.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (f"r365.daily_sales:{location_id}:{business_date}",),
    )
    db.execute(
        "SELECT id FROM r365.daily_sales WHERE location_id = %s AND business_date = %s FOR UPDATE",
        (location_id, business_date),
    )
    previous = db.fetchone()
    if previous is None or previous["id"] == summary_id:
        return None
    previous_id = previous["id"]
    for table in ("sales_payments", "sales_ticket_taxes"):
        db.execute(
            sql.SQL("""
                DELETE FROM r365.{} AS child USING r365.sales_tickets AS ticket
                WHERE child.sales_ticket_id = ticket.id AND ticket.daily_sales_id = %s
            """).format(sql.Identifier(table)),
            (previous_id,),
        )
    db.execute(
        "DELETE FROM r365.sales_details WHERE location_id = %s AND business_date = %s",
        (location_id, business_date),
    )
    db.execute("DELETE FROM r365.sales_tickets WHERE daily_sales_id = %s", (previous_id,))
    db.execute("DELETE FROM r365.daily_sales WHERE id = %s", (previous_id,))
    # sales_account is shared reference data, not owned by this summary.
    return previous_id


def write_to_db(
    df: pd.DataFrame, table_name: str, schema: str, db=None, conflict_columns=("id",)
):
    """Upsert rows; a supplied connection's caller owns commit and reporting."""
    if not schema:
        raise ValueError("schema is required")

    if not table_name:
        raise ValueError("table_name is required")

    if df.empty:
        print(f"No rows to write to {schema}.{table_name}.")
        return 0

    if not conflict_columns or any(col not in df.columns for col in conflict_columns):
        raise ValueError(f"{schema}.{table_name} must contain its conflict columns")

    owns_connection = db is None
    with DatabaseConnection() if owns_connection else nullcontext(db) as db:
        table_ref = sql.Identifier(schema, table_name)

        columns = [sql.Identifier(col) for col in df.columns]

        update_columns = [col for col in df.columns if col not in conflict_columns]

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
            sql.SQL(", ").join(sql.Identifier(col) for col in conflict_columns),
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


def fiscal_date_ranges(year, periods, week=None):
    return fiscal_calendar.fiscal_date_ranges(
        year, periods, week, connection_factory=DatabaseConnection,
    )


def positive_integer(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def build_parser():
    parser = argparse.ArgumentParser(description="Sync R365 changes or bulk business dates.")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--daily", nargs="?", const=True, type=date.fromisoformat,
                       metavar="YYYY-MM-DD", help="Sync changes yesterday (or on the supplied date).")
    modes.add_argument("--week", "--weekly", action="store_true",
                       help="Sync each day from Wednesday through yesterday.")
    modes.add_argument("--bulk", action="store_true", help="Import dated tables by business date.")
    parser.add_argument("--business-date", type=date.fromisoformat, metavar="YYYY-MM-DD",
                        help="Single business date for --bulk; alternative to fiscal selections.")
    parser.add_argument("-y", "--year", type=positive_integer, help="Fiscal year for bulk import.")
    parser.add_argument("-p", "--period", type=positive_integer, nargs="+", help="Fiscal periods (default: whole year).")
    parser.add_argument("-w", "--fiscal-week", type=positive_integer, help="Week within one fiscal period.")
    parser.add_argument("--tables", "--sync", nargs="+",
                        choices=[*DAILY_TABLES, *REFERENCE_TABLES, *CSV_TABLES, "sales_details", "sales_tickets", "all"],
                        help="Endpoints to sync; sales selections include all related sales tables.")
    return parser


DATED_LABOR_TABLES = ("labor_punches", "payroll_summaries", "sales_analytics")


DAILY_TABLES = (
    "daily_sales", "inventory_counts", "transactions", "vendor_invoices", *DATED_LABOR_TABLES,
    # "invoices",  # Accounting invoice sync retained for future use.
)


def sync_daily_tables(client, start, end, tables=DAILY_TABLES):
    """Sync dated records and their children using inclusive record dates."""
    print(f"Syncing daily tables from {start} through {end}.")
    if any(table in tables for table in DATED_LABOR_TABLES):
        sync_dated_labor(client, start, end, tables)
    if "daily_sales" in tables:
        sync_daily_sales(client, business_date_start=start, business_date_end=end)
    if "inventory_counts" in tables:
        write_to_db(r365_inventory_counts(client, start, end), "inventory_counts", "r365")
    if "transactions" in tables:
        location_ids = [row["id"] for row in get_locations(client)]
        write_to_db(
            r365_transactions(client, location_ids, start, end), "transactions", "r365"
        )
    # Accounting invoices are disabled in favor of vendor_invoices.
    # To reactivate, also uncomment "invoices" in DAILY_TABLES.
    # if "invoices" in tables:
    #     sync_invoices(client, start, end)
    if "vendor_invoices" in tables:
        sync_vendor_invoices(client, start, end)


REFERENCE_TABLES = (
    "locations", "units_of_measure", "item_categories", "gl_accounts", "purchase_items",
    "vendors", "vendor_items", "jobs", "employees", "users",
)


CSV_TABLES = ("pos_items", "menu_items", "recipe_items", "ingredients")


def load_csv_tables(tables):
    """Validate all selected exports before writing any of them."""
    return {
        table: fetch() for table, fetch in (
            ("pos_items", get_pos_items_csv), ("menu_items", get_menu_items_csv),
            ("recipe_items", get_recipe_items_csv), ("ingredients", get_ingredients_csv),
        ) if table in tables
    }


def sync_csv_tables(frames):
    """Write normalized CSV/API frames together using the source UUIDs."""
    if not frames:
        return
    counts = {}
    with DatabaseConnection() as db:
        for table, frame in frames.items():
            keys = {"conflict_columns": INGREDIENT_KEYS} if table == "ingredients" else {}
            counts[table] = write_to_db(frame, table, "r365", db=db, **keys)
    for table, count in counts.items():
        print(f"r365.{table}: fetched={len(frames[table]):,}, upserted={count:,}.")


def sync_weekly_tables(client, tables=REFERENCE_TABLES):
    """Refresh selected reference endpoints once, independently of the date range."""
    recipe_items = get_recipe_items_csv()
    ingredients = get_ingredients_csv()
    sync_recipe_tables(recipe_items, ingredients)
    locations = None
    if "locations" in tables or "employees" in tables:
        locations = r365_locations(client)
    if "locations" in tables:
        write_to_db(locations, "locations", "r365")
    for table, fetch in (
        ("units_of_measure", r365_units_of_measure),
        ("item_categories", r365_item_categories),
        ("gl_accounts", r365_gl_accounts),
        ("purchase_items", r365_purchase_items),
        ("vendors", r365_vendors),
    ):
        if table in tables:
            write_to_db(fetch(client), table, "r365")
    if "vendor_items" in tables:
        sync_vendor_items(client, full_download=True)
    # Reference endpoints require both boundaries to retrieve their full history.
    start, end = date.min.isoformat(), date.today().isoformat()
    if "jobs" in tables:
        sync_jobs(client, start, end)
    if "employees" in tables:
        sync_employees(client, locations["id"].to_list(), start, end)
    if "users" in tables:
        sync_users(client)


BULK_TABLES = ("daily_sales", "transactions", "vendor_invoices", *DATED_LABOR_TABLES)


def modified_day_bounds(day):
    """Cover the entire selected day in the runtime's local timezone."""
    start = datetime.combine(day, datetime.min.time()).astimezone()
    end = datetime.combine(day, datetime.max.time()).astimezone()
    return {"modified_on_start": start.isoformat(), "modified_on_end": end.isoformat()}


def sync_modified_tables(client, day, tables, location_ids=None):
    dates = modified_day_bounds(day)
    print(f"Syncing changes on {day}: {dates['modified_on_start']} through {dates['modified_on_end']}.")
    for table, fetch in (
        ("locations", r365_locations), ("units_of_measure", r365_units_of_measure),
        ("item_categories", r365_item_categories), ("gl_accounts", r365_gl_accounts),
        ("purchase_items", r365_purchase_items), ("vendors", r365_vendors),
        ("inventory_counts", r365_inventory_counts),
    ):
        if table in tables:
            write_to_db(fetch(client, **dates), table, "r365")
    if "vendor_items" in tables:
        sync_vendor_items(client, **dates)
    # Labor endpoints use inclusive calendar dates, not date-time filters.
    if "jobs" in tables:
        sync_jobs(client, day.isoformat(), day.isoformat())
    if "employees" in tables and location_ids:
        sync_employees(client, location_ids, day.isoformat(), day.isoformat())
    if "users" in tables:
        sync_users(client, **dates)
    if "transactions" in tables:
        write_to_db(r365_transactions(client, location_ids or [], **dates), "transactions", "r365")
    if "vendor_invoices" in tables:
        sync_vendor_invoices(client, **dates)
    if any(table in tables for table in DATED_LABOR_TABLES):
        sync_dated_labor(client, day, day, tables, modified_dates=dates)
    if "daily_sales" in tables:
        print("Daily sales supports businessDate only; older sales changes require a bulk rerun.")
        sync_daily_sales(client, business_date_start=day, business_date_end=day)


def sync_recipe_tables(recipe_items, ingredients):
    """Upsert normalized recipe data independently of its CSV/API source."""
    with DatabaseConnection() as db:
        recipe_count = write_to_db(recipe_items, "recipe_items", "r365", db=db)
        ingredient_count = write_to_db(
            ingredients, "ingredients", "r365", db=db, conflict_columns=INGREDIENT_KEYS
        )
    print(f"Recipe items fetched/upserted={recipe_count:,}; "
          f"ingredients fetched/upserted={ingredient_count:,}.")


def main(argv=None):
    parser = build_parser()
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in {"daily", "weekly", "bulk"}:
        argv[0] = "--" + argv[0]
    # Preserve numeric --week in existing fiscal bulk commands.
    for i, arg in enumerate(argv[:-1]):
        if arg == "--week" and argv[i + 1].isdigit():
            argv[i] = "--fiscal-week"
    args = parser.parse_args(argv)
    bulk = args.bulk or (args.period is not None and args.daily is None and not args.week)
    if bulk:
        if args.business_date is not None:
            if args.year is not None or args.period is not None or args.fiscal_week is not None:
                parser.error("--business-date cannot be combined with fiscal selections")
        elif args.year is None:
            parser.error("--bulk requires --year or --business-date")
        if args.fiscal_week is not None and (not args.period or len(set(args.period)) != 1):
            parser.error("-w/--fiscal-week requires exactly one fiscal period")
    elif args.daily is None and not args.week:
        parser.error("choose --daily, --week, or --bulk")
    elif any(value is not None for value in (args.year, args.period, args.fiscal_week, args.business_date)):
        parser.error("business-date and fiscal selections require --bulk")
    defaults = (list(BULK_TABLES) if bulk else
                [*REFERENCE_TABLES, *DAILY_TABLES, *(CSV_TABLES if args.week else ())])
    tables = args.tables or defaults
    if "all" in tables:
        tables = defaults
    tables = list(dict.fromkeys("daily_sales" if t in ("sales_details", "sales_tickets") else t for t in tables))
    if bulk and any(t not in DAILY_TABLES for t in tables):
        parser.error("--bulk accepts dated tables only; reference tables use --daily or --week")
    frames = load_csv_tables(tables) if any(t in CSV_TABLES for t in tables) else {}
    tables = [table for table in tables if table not in CSV_TABLES]
    if not tables:
        sync_csv_tables(frames)
        return
    try:
        if bulk:
            ranges = ([(args.business_date, args.business_date)] if args.business_date else
                      fiscal_date_ranges(args.year, args.period, args.fiscal_week))
        elif args.week:
            ranges = [fiscal_calendar.fiscal_week_to_date(
                today=date.today(), connection_factory=DatabaseConnection,
            )]
        else:
            ranges = [fiscal_calendar.daily_date_range(None if args.daily is True else args.daily)]
    except ValueError as exc:
        parser.error(str(exc))
    sync_csv_tables(frames)
    client = R365Client()
    # Scoping must include unchanged locations, even when their delta is empty.
    location_ids = None
    if not bulk and any(t in tables for t in ("employees", "transactions")):
        location_ids = [row["id"] for row in get_locations(client)]
    for start, end in ranges:
        day = start
        while day <= end:
            if bulk:
                sync_daily_tables(client, day, day, tables)
            else:
                sync_modified_tables(client, day, tables, location_ids)
            day += timedelta(days=1)


if __name__ == "__main__":
    main()
