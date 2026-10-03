"""Normalize the Orders API into seven source tables, one restaurant/day at a time."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import logging

import requests

from db_utils.dbconnect import DatabaseConnection
from src.toast_sync.writing import upsert_rows
from src.toast_sync.dates import business_date_options
from src.toast_sync.values import (
    source_uuid, source_timestamp, source_numeric, source_value, source_array,
)

logger = logging.getLogger(__name__)

TEXT_FIELDS = {
    "external_id": "externalId", "display_number": "displayNumber", "source": "source",
    "approval_status": "approvalStatus", "created_by_client_name": "createdByClientName",
    "required_prep_time": "requiredPrepTime",
}
INTEGER_FIELDS = {"number_of_guests": "numberOfGuests", "duration": "duration"}
TIMESTAMP_FIELDS = {
    "opened_date": "openedDate", "modified_date": "modifiedDate",
    "promised_date": "promisedDate", "created_date": "createdDate",
    "paid_date": "paidDate", "closed_date": "closedDate", "deleted_date": "deletedDate",
    "void_date": "voidDate", "estimated_fulfillment_date": "estimatedFulfillmentDate",
}
BOOLEAN_FIELDS = {
    "voided": "voided", "deleted": "deleted", "created_in_test_mode": "createdInTestMode",
    "excess_food": "excessFood",
}
REFERENCE_FIELDS = {
    "server_id": "server", "dining_option_id": "diningOption", "table_id": "table",
    "service_area_id": "serviceArea", "restaurant_service_id": "restaurantService",
    "revenue_center_id": "revenueCenter",
}
ORDER_COLUMNS = (
    "id", "restaurant_id", "business_date", *TEXT_FIELDS, *INTEGER_FIELDS,
    "calculated_guest_count", *TIMESTAMP_FIELDS, "void_business_date", *BOOLEAN_FIELDS,
    *REFERENCE_FIELDS, "channel_id", "created_device_id", "last_modified_device_id",
    "pricing_features", "last_synced_at",
)


def get_options(args):
    """Validate CLI dates before opening a connection or authenticating."""
    if "orders" not in args.sync:
        return {}
    return business_date_options(args, required_for="Orders")


def source_business_date(value, field):
    if type(value) is not int or len(str(value)) != 8:
        raise ValueError(f"{field} must be an integer yyyyMMdd")
    try:
        return datetime.strptime(str(value), "%Y%m%d").date()
    except ValueError:
        raise ValueError(f"{field} is not a valid calendar date") from None


def normalize_order(payload, restaurant_id, business_date=None, synced_at=None):
    """Normalize header fields only; never derive the business date from a timestamp.

    Field definitions: https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/Order/
    """
    if not isinstance(payload, dict):
        raise ValueError("Expected an Order object")
    row = {
        "id": source_uuid(payload.get("guid"), "guid", required=True),
        "restaurant_id": source_uuid(restaurant_id, "restaurant_id", required=True),
        "business_date": source_business_date(payload.get("businessDate"), "businessDate"),
    }
    if business_date is not None and row["business_date"] != business_date:
        raise ValueError("Returned businessDate differs from requested business date")
    for fields, expected_type in ((TEXT_FIELDS, str), (INTEGER_FIELDS, int), (BOOLEAN_FIELDS, bool)):
        for column, field in fields.items():
            value = payload.get(field)
            if value is not None and type(value) is not expected_type:
                raise ValueError(f"{field} must be {expected_type.__name__} or null")
            row[column] = value
    for column, field in TIMESTAMP_FIELDS.items():
        row[column] = source_timestamp(payload.get(field), field)
    for column, field in REFERENCE_FIELDS.items():
        reference = payload.get(field)
        if reference is not None and not isinstance(reference, dict):
            raise ValueError(f"{field} must be a reference object or null")
        row[column] = source_uuid((reference or {}).get("guid"), f"{field}.guid")
    row["channel_id"] = source_uuid(payload.get("channelGuid"), "channelGuid")
    for column, field in (("created_device_id", "createdDevice"), ("last_modified_device_id", "lastModifiedDevice")):
        device = payload.get(field)
        if device is not None and not isinstance(device, dict):
            raise ValueError(f"{field} must be a device object or null")
        value = (device or {}).get("id")
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{field}.id must be a string or null")
        row[column] = value

    guest_count = payload.get("calculatedGuestCount")
    row["calculated_guest_count"] = None
    if guest_count is not None:
        try:
            if isinstance(guest_count, bool) or not isinstance(guest_count, (int, float, Decimal)):
                raise ValueError
            guest_count = Decimal(str(guest_count))
            if not guest_count.is_finite():
                raise ValueError
        except (InvalidOperation, ValueError):
            raise ValueError("calculatedGuestCount must be a finite number or null") from None
        row["calculated_guest_count"] = guest_count
    void_date = payload.get("voidBusinessDate")
    # Toast uses 0 for an unset void business date; it cannot be represented as DATE.
    if void_date is None or (type(void_date) is int and void_date == 0):
        row["void_business_date"] = None
    else:
        row["void_business_date"] = source_business_date(void_date, "voidBusinessDate")
    features = payload.get("pricingFeatures")
    if features is not None and (not isinstance(features, list) or any(not isinstance(item, str) for item in features)):
        raise ValueError("pricingFeatures must be an array of strings or null")
    row["pricing_features"] = features
    row["last_synced_at"] = synced_at or datetime.now(timezone.utc)
    return row


def upsert_orders(rows):
    return upsert_rows(rows, "orders", ORDER_COLUMNS)


ENDPOINT = "/orders/v2/ordersBulk"
CONTEXT_COLUMNS = ("order_id", "restaurant_id", "business_date")
# Dotted paths refer only to documented source objects, not calculated values.
CHILD_FIELDS = {
    "checks": {
        "text": {"display_number": "displayNumber", "tab_name": "tabName", "payment_status": "paymentStatus"},
        "timestamp": {"created_date": "createdDate", "opened_date": "openedDate", "closed_date": "closedDate", "modified_date": "modifiedDate", "deleted_date": "deletedDate", "paid_date": "paidDate", "void_date": "voidDate"},
        "numeric": {"amount": "amount", "tax_amount": "taxAmount", "total_amount": "totalAmount"},
        "boolean": {"tax_exempt": "taxExempt", "voided": "voided", "deleted": "deleted"},
        "date": {"void_business_date": "voidBusinessDate"},
        "integer": {"duration": "duration"},
        "uuid": {"opened_by_id": "openedBy.guid"},
    },
    "selections": {
        "uuid": {"item_id": "item.guid", "item_group_id": "itemGroup.guid", "option_group_id": "optionGroup.guid", "pre_modifier_id": "preModifier.guid", "sales_category_id": "salesCategory.guid", "dining_option_id": "diningOption.guid", "void_reason_id": "voidReason.guid", "refund_transaction_id": "refundDetails.refundTransaction.guid"},
        "text": {"display_name": "displayName", "plu": "plu", "pre_modifier_plu": "premodifierPlu", "sales_category_plu": "salesCategoryPlu", "unit_of_measure": "unitOfMeasure", "selection_type": "selectionType", "fulfillment_status": "fulfillmentStatus", "tax_inclusion": "taxInclusion", "option_group_pricing_mode": "optionGroupPricingMode"},
        "numeric": {"quantity": "quantity", "pre_discount_price": "preDiscountPrice", "price": "price", "receipt_line_price": "receiptLinePrice", "open_price_amount": "openPriceAmount", "external_price_amount": "externalPriceAmount", "tax": "tax", "guest_count_weight": "guestCountWeight", "refund_amount": "refundDetails.refundAmount", "tax_refund_amount": "refundDetails.taxRefundAmount"},
        "integer": {"seat_number": "seatNumber"},
        "boolean": {"voided": "voided", "deferred": "deferred"},
        "timestamp": {"void_date": "voidDate", "created_date": "createdDate", "modified_date": "modifiedDate"},
        "date": {"void_business_date": "voidBusinessDate"},
    },
    "payments": {
        "timestamp": {"paid_date": "paidDate", "refund_date": "refund.refundDate", "void_date": "voidInfo.voidDate"},
        "date": {"paid_business_date": "paidBusinessDate", "refund_business_date": "refund.refundBusinessDate", "void_business_date": "voidInfo.voidBusinessDate"},
        "text": {"type": "type", "card_entry_mode": "cardEntryMode", "card_type": "cardType", "refund_status": "refundStatus", "payment_status": "paymentStatus"},
        "numeric": {"amount": "amount", "tip_amount": "tipAmount", "amount_tendered": "amountTendered", "refund_amount": "refund.refundAmount", "tip_refund_amount": "refund.tipRefundAmount"},
        "uuid": {"server_id": "server.guid", "cash_drawer_id": "cashDrawer.guid", "house_account_id": "houseAccount.guid", "other_payment_id": "otherPayment.guid", "refund_transaction_id": "refund.refundTransaction.guid", "void_reason_id": "voidInfo.voidReason.guid"},
    },
    "applied_discounts": {
        "uuid": {"discount_id": "discount.guid", "approver_id": "approver.guid", "discount_reason_id": "appliedDiscountReason.discountReason.guid"},
        "text": {"name": "name", "discount_type": "discountType", "discount_plu": "discountPlu", "processing_state": "processingState", "applied_promo_code": "appliedPromoCode", "reason_name": "appliedDiscountReason.name", "reason_description": "appliedDiscountReason.description", "reason_comment": "appliedDiscountReason.comment"},
        "numeric": {"discount_amount": "discountAmount", "non_tax_discount_amount": "nonTaxDiscountAmount", "discount_percent": "discountPercent"},
    },
    "service_charges": {
        "uuid": {"service_charge_id": "serviceCharge.guid", "payment_id": "paymentGuid", "refund_transaction_id": "refundDetails.refundTransaction.guid"},
        "text": {"name": "name", "charge_type": "chargeType", "service_charge_calculation": "serviceChargeCalculation", "service_charge_category": "serviceChargeCategory"},
        "numeric": {"charge_amount": "chargeAmount", "refund_amount": "refundDetails.refundAmount", "tax_refund_amount": "refundDetails.taxRefundAmount"},
        "boolean": {"delivery": "delivery", "takeout": "takeout", "dine_in": "dineIn", "gratuity": "gratuity", "taxable": "taxable"},
    },
    "applied_taxes": {
        "uuid": {"tax_rate_id": "taxRate.guid"},
        "text": {"name": "name", "display_name": "displayName", "type": "type", "jurisdiction": "jurisdiction", "jurisdiction_type": "jurisdictionType"},
        "numeric": {"rate": "rate", "tax_amount": "taxAmount"},
        "boolean": {"facilitator_collect_and_remit_tax": "facilitatorCollectAndRemitTax"},
    },
}
PARENT_COLUMNS = {
    "checks": (),
    "selections": ("check_id", "parent_selection_id"),
    "payments": ("check_id",),
    "applied_discounts": ("check_id", "selection_id"),
    "service_charges": ("check_id",),
    "applied_taxes": ("check_id", "selection_id", "service_charge_id", "tax_inclusion"),
}
TABLE_COLUMNS = {"orders": ORDER_COLUMNS}
for _table, _groups in CHILD_FIELDS.items():
    TABLE_COLUMNS[_table] = (
        "id", *CONTEXT_COLUMNS, *PARENT_COLUMNS[_table],
        *(column for fields in _groups.values() for column in fields), "last_synced_at",
    )


def source_optional_date(value, field):
    if value is None or (type(value) is int and value == 0):
        return None
    return source_business_date(value, field)


def normalize_child(table, payload, context):
    """Shared scalar validation; errors carry safe parent IDs, never payloads."""
    try:
        if not isinstance(payload, dict):
            raise ValueError("Expected an object")
        row = {"id": source_uuid(payload.get("guid"), "guid", required=True)}
        row.update({column: context[column] for column in (*CONTEXT_COLUMNS, *PARENT_COLUMNS[table], "last_synced_at")})
        converters = {"uuid": source_uuid, "numeric": source_numeric,
                      "timestamp": source_timestamp, "date": source_optional_date}
        for kind, fields in CHILD_FIELDS[table].items():
            for column, field in fields.items():
                value = source_value(payload, field)
                if kind in converters:
                    value = converters[kind](value, field)
                elif value is not None and type(value) is not {"text": str, "integer": int, "boolean": bool}[kind]:
                    raise ValueError(f"{field} must be {kind} or null")
                row[column] = value
        return row
    except ValueError as exc:
        raise ValueError(
            f"{table} order={context['order_id']} check={context.get('check_id')} "
            f"selection={context.get('selection_id')} service_charge={context.get('service_charge_id')}: {exc}"
        ) from None


def normalize_check(payload, context):
    return normalize_child("checks", payload, context)


def normalize_selection(payload, context, parent_selection_id=None):
    return normalize_child("selections", payload, {**context, "parent_selection_id": parent_selection_id})


def normalize_payment(payload, context):
    row = normalize_child("payments", payload, context)
    for field, column in (("orderGuid", "order_id"), ("checkGuid", "check_id")):
        value = payload.get(field)
        if value is not None and source_uuid(value, field) != context[column]:
            raise ValueError(f"Payment {row['id']} {field} differs from parent order={context['order_id']} check={context['check_id']}")
    return row


def normalize_discount(payload, context, selection_id=None):
    return normalize_child("applied_discounts", payload, {**context, "selection_id": selection_id})


def normalize_service_charge(payload, context):
    return normalize_child("service_charges", payload, context)


def normalize_tax(payload, context, selection_id=None, service_charge_id=None, tax_inclusion=None):
    if (selection_id is None) == (service_charge_id is None):
        raise ValueError("Applied tax must have exactly one selection or service-charge parent")
    return normalize_child("applied_taxes", payload, {
        **context, "selection_id": selection_id, "service_charge_id": service_charge_id,
        "tax_inclusion": tax_inclusion,
    })


def walk_selections(selections, context):
    """Depth-first modifier traversal adapted from product-mix process_modifiers.

    An explicit stack preserves arbitrary modifier depth without Python's call
    stack limit. Emit each selection with the GUID of its immediate parent.
    """
    stack = [(item, None) for item in reversed(selections)]
    seen = set()
    while stack:
        payload, parent_id = stack.pop()
        row = normalize_selection(payload, context, parent_id)
        if row["id"] in seen:
            raise ValueError(f"Repeated/cyclic selection={row['id']} order={context['order_id']} check={context['check_id']}")
        seen.add(row["id"])
        yield payload, row
        stack.extend((modifier, row["id"]) for modifier in reversed(source_array(payload, "modifiers")))


def flatten_order(payload, restaurant_id, business_date=None, synced_at=None):
    """Normalize one whole order once. Any malformed child rejects this order."""
    header = normalize_order(payload, restaurant_id, business_date, synced_at)
    tables = {table: [] for table in TABLE_COLUMNS}
    tables["orders"].append(header)
    context = {"order_id": header["id"], "restaurant_id": header["restaurant_id"],
               "business_date": header["business_date"], "last_synced_at": header["last_synced_at"]}
    check_id = None
    try:
        for check in source_array(payload, "checks"):
            check_id = None
            check_row = normalize_check(check, context)
            check_id = check_row["id"]
            tables["checks"].append(check_row)
            check_context = {**context, "check_id": check_id}
            for discount in source_array(check, "appliedDiscounts"):
                tables["applied_discounts"].append(normalize_discount(discount, check_context))
            for payment in source_array(check, "payments"):
                tables["payments"].append(normalize_payment(payment, check_context))
            for charge in source_array(check, "appliedServiceCharges"):
                charge_row = normalize_service_charge(charge, check_context)
                tables["service_charges"].append(charge_row)
                for tax in source_array(charge, "appliedTaxes"):
                    tables["applied_taxes"].append(normalize_tax(tax, check_context, service_charge_id=charge_row["id"]))
            for selection, selection_row in walk_selections(source_array(check, "selections"), check_context):
                selection_id = selection_row["id"]
                tables["selections"].append(selection_row)
                for discount in source_array(selection, "appliedDiscounts"):
                    tables["applied_discounts"].append(normalize_discount(discount, check_context, selection_id))
                for tax in source_array(selection, "appliedTaxes"):
                    tables["applied_taxes"].append(normalize_tax(
                        tax, check_context, selection_id=selection_id, tax_inclusion=selection_row["tax_inclusion"],
                    ))
        # A GUID must never silently overwrite a different application/parent.
        for table, rows in tables.items():
            unique = {}
            for row in rows:
                if row["id"] in unique and row != unique[row["id"]]:
                    raise ValueError(f"Conflicting {table} id={row['id']}")
                unique[row["id"]] = row
            tables[table] = list(unique.values())
    except ValueError as exc:
        raise ValueError(f"order={header['id']} check={check_id}: {exc}") from None
    return tables


def fetch_orders(client, restaurant_id, business_date):
    payload = client.get_paged_response_data(
        ENDPOINT, str(restaurant_id), page_size=100, parse_float=Decimal,
        params={"businessDate": business_date.strftime("%Y%m%d")},
    )
    if not isinstance(payload, list):
        raise ValueError("Expected orders array")
    return payload


def upsert_order_tables(tables):
    """Commit all seven tables together; publish counts only after commit."""
    if not tables["orders"]:
        return {table: {"inserted": 0, "updated": 0} for table in TABLE_COLUMNS}
    counts = {}
    with DatabaseConnection() as db:
        for table, columns in TABLE_COLUMNS.items():
            try:
                counts[table] = upsert_rows(tables[table], table, columns, db=db)
            except Exception as exc:
                logger.error("Database table=toast.%s error=%s SQLSTATE=%s", table, type(exc).__name__, getattr(exc, "pgcode", None))
                raise
    return counts


def normalize_orders(payload, restaurant_id, business_date, synced_at):
    """Deduplicate complete normalized order graphs, including their children."""
    graphs = {}
    conflicts = set()
    rejected = duplicates = 0
    for item in payload:
        try:
            graph = flatten_order(item, restaurant_id, business_date, synced_at)
        except ValueError as exc:
            rejected += 1
            logger.error("endpoint=%s restaurant=%s business_date=%s rejected: %s", ENDPOINT, restaurant_id, business_date, exc)
            # If another copy of this order was valid, do not write an ambiguous snapshot.
            try:
                conflicts.add(source_uuid(item.get("guid"), "guid", required=True))
            except (AttributeError, ValueError):
                pass
            continue
        order_id = graph["orders"][0]["id"]
        if order_id in graphs:
            duplicates += 1
            # Compare by IDs so harmless changes to API array ordering don't conflict.
            if any({r["id"]: r for r in graph[t]} != {r["id"]: r for r in graphs[order_id][t]} for t in TABLE_COLUMNS):
                conflicts.add(order_id)
        else:
            graphs[order_id] = graph
    for order_id in conflicts:
        if order_id in graphs:
            del graphs[order_id]
            rejected += 1
            logger.error("endpoint=%s restaurant=%s business_date=%s conflicting order=%s; skipped", ENDPOINT, restaurant_id, business_date, order_id)
    tables = {table: {} for table in TABLE_COLUMNS}
    for graph in graphs.values():
        for table, rows in graph.items():
            for row in rows:
                if row["id"] in tables[table] and row != tables[table][row["id"]]:
                    raise ValueError(f"Conflicting {table} id={row['id']} across orders {tables[table][row['id']].get('order_id')} and {row.get('order_id')}")
                tables[table][row["id"]] = row
    return {table: list(rows.values()) for table, rows in tables.items()}, rejected, duplicates


def sync_orders(client, locations, dry_run=False, *, business_date_start, business_date_end):
    if business_date_end < business_date_start:
        raise ValueError("Business-date end must be on or after start")
    counts = {"requested": 0, "returned": 0, "rejected": 0, "duplicates": 0,
              "api_errors": 0, "database_errors": 0, "inserted": 0, "updated": 0,
              "tables": {table: {"inserted": 0, "updated": 0} for table in TABLE_COLUMNS}}
    for location in locations:
        guid = str(location["toast_guid"])
        for offset in range((business_date_end - business_date_start).days + 1):
            business_date = business_date_start + timedelta(days=offset)
            context = f"endpoint={ENDPOINT} restaurant={guid} business_date={business_date}"
            counts["requested"] += 1
            try:
                payload = fetch_orders(client, guid, business_date)
            except (requests.RequestException, ValueError) as exc:
                counts["api_errors"] += 1
                response = getattr(exc, "response", None)
                logger.error("%s API error=%s HTTP=%s; no rows written", context, type(exc).__name__, response.status_code if response is not None else "unavailable")
                continue
            counts["returned"] += len(payload)
            try:
                tables, rejected, duplicates = normalize_orders(payload, guid, business_date, datetime.now(timezone.utc))
            except ValueError as exc:
                counts["rejected"] += len(payload)
                logger.error("%s rejected batch: %s", context, exc)
                continue
            counts["rejected"] += rejected
            counts["duplicates"] += duplicates
            logger.info("%s fetched=%d valid_orders=%d rejected=%d dry_run=%s", context, len(payload), len(tables["orders"]), rejected, dry_run)
            if dry_run:
                continue
            try:
                written = upsert_order_tables(tables)
            except Exception as exc:
                counts["database_errors"] += 1
                logger.error("%s database error=%s SQLSTATE=%s; transaction not confirmed", context, type(exc).__name__, getattr(exc, "pgcode", None))
                continue
            for table, result in written.items():
                for key in ("inserted", "updated"):
                    counts["tables"][table][key] += result[key]
            # Retain the existing top-level order counts for callers.
            counts["inserted"] += written["orders"]["inserted"]
            counts["updated"] += written["orders"]["updated"]
    logger.info("Toast Orders Update: restaurants=%d batches=%d fetched=%d rejected=%d duplicates=%d API_errors=%d database_errors=%d dry_run=%s",
                len(locations), counts["requested"], counts["returned"], counts["rejected"], counts["duplicates"], counts["api_errors"], counts["database_errors"], dry_run)
    logger.info("%-26s %10s %10s", "Table", "Inserted", "Updated")
    for table, result in counts["tables"].items():
        logger.info("%-26s %10d %10d", f"toast.{table}", result["inserted"], result["updated"])
    return counts


sync = sync_orders
