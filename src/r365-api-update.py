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
)
from db_utils.dbconnect import DatabaseConnection
from psycopg2 import sql
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
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
    """Use today by default; one supplied boundary means that single day."""
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


def r365_employee_map(employees):
    records = []
    for row in employees:
        pos_employee_id = convert_uuid(row.get("posEmployeeId"))
        if pos_employee_id is not None:
            records.append(
                {
                    "pos_employee_id": pos_employee_id,
                    "employee_id": required_uuid(row, "employeeId"),
                }
            )
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
    missing_pos = sum(row.get("posEmployeeId") is None for row in employees)
    if missing_pos:
        print(
            f"Skipped {missing_pos:,} mappings without a POS employee ID; employees retained."
        )


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


def r365_users(client):
    """The endpoint omits allReportsAccess; leave its database value/default intact."""
    users = get_users(client)
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


def sync_users(client):
    write_to_db(r365_users(client), "users", "r365")


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
                # The header upsert locks this summary until all children are committed.
                counts["daily_sales"] = write_to_db(
                    summary, "daily_sales", "r365", db=db
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
            print(
                f"Daily sales {location_id} {business_date} ({len(detail_rows):,} source item lines): "
                + ", ".join(
                    f"{table} fetched/upserted={count:,}"
                    for table, count in counts.items()
                )
            )
            business_date += timedelta(days=1)


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--full-vendor-items",
        action="store_true",
        help="Download and upsert all vendor items only, without date filters.",
    )
    mode.add_argument(
        "--sync",
        nargs="+",
        choices=["employees", "jobs", "users", "daily-sales"],
        help="Sync only the selected resources, including employee mappings for employees.",
    )
    parser.add_argument(
        "--modified-on-start",
        type=date.fromisoformat,
        help="Labor modification start date (YYYY-MM-DD); defaults to today.",
    )
    parser.add_argument(
        "--modified-on-end",
        type=date.fromisoformat,
        help="Labor modification end date (YYYY-MM-DD); one boundary selects a single day.",
    )
    parser.add_argument(
        "--business-date-start",
        type=date.fromisoformat,
        help="Daily sales start date (YYYY-MM-DD); defaults to seven days ago.",
    )
    parser.add_argument(
        "--business-date-end",
        type=date.fromisoformat,
        help="Daily sales end date (YYYY-MM-DD); defaults to yesterday.",
    )
    args = parser.parse_args()
    if (args.business_date_start or args.business_date_end) and (
        args.full_vendor_items or (args.sync and "daily-sales" not in args.sync)
    ):
        parser.error("Business-date filters require the daily-sales sync")
    try:
        sales_start, sales_end = daily_sales_date_range(
            args.business_date_start, args.business_date_end
        )
        start, end = labor_date_range(args.modified_on_start, args.modified_on_end)
    except ValueError as exc:
        parser.error(str(exc))
    client = R365Client()

    if args.full_vendor_items:
        sync_vendor_items(client, full_download=True)
        raise SystemExit(0)

    if args.sync:
        if "daily-sales" in args.sync:
            sync_daily_sales(
                client,
                business_date_start=sales_start,
                business_date_end=sales_end,
            )
        if "jobs" in args.sync:
            sync_jobs(client, start, end)
        if "employees" in args.sync:
            location_ids = [row["id"] for row in get_locations(client)]
            sync_employees(client, location_ids, start, end)
        if "users" in args.sync:
            sync_users(client)
        raise SystemExit(0)

    locations_df = r365_locations(client)
    location_ids = locations_df["id"].to_list()
    write_to_db(locations_df, "locations", "r365")

    sync_daily_sales(
        client,
        business_date_start=sales_start,
        business_date_end=sales_end,
    )

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
    # sync_vendor_invoices(client)
    # sync_vendor_items(client)
    # sync_jobs(client, start, end)
    # sync_employees(client, location_ids, start, end)
    # sync_users(client)
