"""Restaurant-scoped Configuration v2 imports; see docs/toast-configuration.md."""

import json
import logging
import time
from datetime import UTC, datetime

import requests
from psycopg2 import Error as DatabaseError
from psycopg2.extras import Json

from src.toast_sync.values import (
    source_numeric,
    source_timestamp,
    source_uuid,
    source_value,
)
from src.toast_sync.writing import upsert_rows

logger = logging.getLogger(__name__)
ENDPOINTS = {
    "alternate_payment_types": "alternatePaymentTypes",
    "break_types": "breakTypes",
    "cash_drawers": "cashDrawers",
    "dining_options": "diningOptions",
    "discounts": "discounts",
    "menu_groups": "menuGroups",
    "menu_items": "menuItems",
    "menu_option_groups": "menuOptionGroups",
    "menus": "menus",
    "no_sale_reasons": "noSaleReasons",
    "payout_reasons": "payoutReasons",
    "pre_modifier_groups": "preModifierGroups",
    "pre_modifiers": "preModifiers",
    "price_groups": "priceGroups",
    "printers": "printers",
    "restaurant_services": "restaurantServices",
    "revenue_centers": "revenueCenters",
    "sales_categories": "salesCategories",
    "service_areas": "serviceAreas",
    "service_charges": "serviceCharges",
    "tables": "tables",
    "tax_rates": "taxRates",
    "tip_withholding": "tipWithholding",
    "void_reasons": "voidReasons",
}
COMMON_FIELDS = {
    "entity_type": ("entityType", "text"),
    "external_id": ("externalId", "text"),
}
NAME = {"name": ("name", "text")}
MENU_FIELDS = {
    **NAME,
    "orderable_online": ("orderableOnline", "text"),
    "visibility": ("visibility", "text"),
    "unit_of_measure": ("unitOfMeasure", "text"),
    "images": ("images", "array"),
}
OPTION_FIELDS = {
    "option_groups": ("optionGroups", "array"),
    "inherit_option_groups": ("inheritOptionGroups", "boolean"),
    "inherit_unit_of_measure": ("inheritUnitOfMeasure", "boolean"),
}
FIELDS = {
    "alternate_payment_types": NAME,
    "break_types": {
        **NAME, "active": ("active", "boolean"), "paid": ("paid", "boolean"),
        "duration": ("duration", "integer"),
        "enforce_minimum_time": ("enforceMinimumTime", "boolean"),
        "track_missed_breaks": ("trackMissedBreaks", "boolean"),
        "break_interval_hrs": ("breakIntervalHrs", "integer"),
        "break_interval_mins": ("breakIntervalMins", "integer"),
        "track_break_acknowledgement": ("trackBreakAcknowledgement", "boolean"),
    },
    "cash_drawers": {"printer": ("printer", "object"), "printer_id": ("printer.guid", "uuid")},
    "dining_options": {
        **NAME, "behavior": ("behavior", "text"), "curbside": ("curbside", "boolean"),
        "drive_thru": ("driveThru", "boolean"),
    },
    "discounts": {
        **NAME, "active": ("active", "boolean"), "type": ("type", "text"),
        "percentage": ("percentage", "numeric"), "amount": ("amount", "numeric"),
        "selection_type": ("selectionType", "text"), "non_exclusive": ("nonExclusive", "boolean"),
        "item_picking_priority": ("itemPickingPriority", "text"),
        "fixed_total": ("fixedTotal", "numeric"), "promo_codes": ("promoCodes", "array"),
    },
    "menu_groups": {
        **MENU_FIELDS, **OPTION_FIELDS,
        "menu": ("menu", "object"), "menu_id": ("menu.guid", "uuid"),
        "parent": ("parent", "object"), "parent_id": ("parent.guid", "uuid"),
        "items": ("items", "array"), "subgroups": ("subgroups", "array"),
    },
    "menu_items": {
        **MENU_FIELDS, **OPTION_FIELDS,
        "calories": ("calories", "integer"), "sku": ("sku", "text"),
        "plu": ("plu", "text"), "type": ("type", "text"),
    },
    "menu_option_groups": {
        **NAME, "options": ("options", "array"),
        "min_selections": ("minSelections", "numeric"), "max_selections": ("maxSelections", "numeric"),
    },
    "menus": {**MENU_FIELDS, "groups": ("groups", "array")},
    "no_sale_reasons": NAME,
    "payout_reasons": NAME,
    "pre_modifier_groups": {**NAME, "is_default": ("isDefault", "boolean")},
    "pre_modifiers": {
        **NAME, "scale_price": ("scalePrice", "boolean"), "base_price": ("basePrice", "numeric"),
        "scale_factor": ("scaleFactor", "numeric"), "display_mode": ("displayMode", "text"),
        "parent": ("parent", "object"), "parent_id": ("parent.guid", "uuid"),
        "charge_as_extra": ("chargeAsExtra", "boolean"),
    },
    "price_groups": NAME,
    "printers": NAME,
    "restaurant_services": NAME,
    "revenue_centers": {**NAME, "description": ("description", "text")},
    "sales_categories": NAME,
    "service_areas": {
        **NAME, "service_area_type": ("serviceAreaType", "text"),
        "revenue_center": ("revenueCenter", "object"), "revenue_center_id": ("revenueCenter.guid", "uuid"),
    },
    "service_charges": {
        **NAME, "amount_type": ("amountType", "text"), "amount": ("amount", "numeric"),
        "percent": ("percent", "numeric"), "criteria": ("criteria", "object"),
        "gratuity": ("gratuity", "boolean"), "taxable": ("taxable", "boolean"),
        "applicable_taxes": ("applicableTaxes", "array"),
        "service_charge_calculation": ("serviceChargeCalculation", "text"),
        "destination": ("destination", "text"), "tax_inclusive": ("taxInclusive", "boolean"),
        "tax_prorated": ("taxProrated", "boolean"),
    },
    "tables": {
        **NAME, "service_area": ("serviceArea", "object"), "service_area_id": ("serviceArea.guid", "uuid"),
        "revenue_center": ("revenueCenter", "object"), "revenue_center_id": ("revenueCenter.guid", "uuid"),
    },
    "tax_rates": {
        **NAME, "is_default": ("isDefault", "boolean"), "rate": ("rate", "numeric"),
        "type": ("type", "text"), "rounding_type": ("roundingType", "text"),
        "tax_table": ("taxTable", "array"), "conditional_tax_rates": ("conditionalTaxRates", "array"),
    },
    "tip_withholding": {"enabled": ("enabled", "boolean"), "percentage": ("percentage", "numeric")},
    "void_reasons": NAME,
}
TABLE_COLUMNS = {
    name: ("restaurant_id", "id", *COMMON_FIELDS, *fields, "last_synced_at")
    for name, fields in FIELDS.items()
}


def add_arguments(parser):
    parser.add_argument("--config-resources", nargs="+", choices=tuple(ENDPOINTS),
                        help="Configuration endpoints to sync (default: all 24; use snake_case names).")
    parser.add_argument("--config-last-modified", metavar="TIMESTAMP",
                        help="Fetch configuration changed after this ISO timestamp with UTC offset; omit for a full load.")


def get_options(args):
    if "configuration" not in args.sync:
        if args.config_resources or args.config_last_modified is not None:
            raise ValueError("Configuration arguments require --sync configuration")
        return {}
    modified = source_timestamp(args.config_last_modified, "config-last-modified")
    return {"resources": tuple(dict.fromkeys(args.config_resources or ENDPOINTS)), "last_modified": modified}


def normalize_record(resource, payload, restaurant_id, synced_at):
    if not isinstance(payload, dict):
        raise TypeError("Configuration record must be an object")
    row = {
        "restaurant_id": source_uuid(restaurant_id, "restaurant_id", required=True),
        # tipWithholding is a singleton per restaurant, even if its source GUID changes.
        "id": source_uuid(payload.get("guid"), "guid", required=resource != "tip_withholding"),
        "last_synced_at": synced_at,
    }
    for column, (field, kind) in {**COMMON_FIELDS, **FIELDS[resource]}.items():
        value = source_value(payload, field)
        if kind == "uuid":
            value = source_uuid(value, field)
        elif kind == "numeric":
            value = source_numeric(value, field)
        elif value is not None:
            expected = {"text": str, "boolean": bool, "integer": int, "array": list, "object": dict}[kind]
            if type(value) is not expected:
                raise ValueError(f"{field} must be {kind} or null")
            if kind == "integer" and not -(2 ** 63) <= value < 2 ** 63:
                raise ValueError(f"{field} exceeds bigint range")
            if kind in {"array", "object"}:
                if kind == "array" and any(not isinstance(item, dict) for item in value):
                    raise ValueError(f"{field} must contain objects")
                # Reject non-JSON values and NaN before any database writes.
                json.dumps(value, allow_nan=False)
                value = Json(value)
        row[column] = value
    return row


def fetch_records(client, resource, restaurant_id, last_modified=None):
    """Strict response shapes and token pagination; never return a partial batch."""
    endpoint = "/config/v2/" + ENDPOINTS[resource]
    params = {}
    if resource == "tip_withholding":
        payload = client.request(endpoint, restaurant_id).json()
        if not isinstance(payload, dict):
            raise ValueError("tipWithholding must return an object")
        # Live API responses use {} when no withholding configuration is returned.
        # Preserve any previously stored row, just as for an empty collection.
        return [payload] if payload else []
    if last_modified is not None:
        if last_modified.tzinfo is None or last_modified.utcoffset() is None:
            raise ValueError("last_modified requires a UTC offset")
        # Configuration v2 expects milliseconds and a compact offset (+0000),
        # rather than Python's default microseconds and colon offset (+00:00).
        utc = last_modified.astimezone(UTC)
        params["lastModified"] = utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}+0000"
    records = []
    seen_tokens = set()
    while True:
        response = client.request(endpoint, restaurant_id, params=params.copy())
        page = response.json()
        if not isinstance(page, list):
            raise TypeError("Configuration collection must return an array")
        records.extend(page)
        token = response.headers.get("Toast-Next-Page-Token")
        if not token:
            return records
        if token in seen_tokens:
            raise ValueError("Configuration pagination repeated a token")
        seen_tokens.add(token)
        params["pageToken"] = token
        time.sleep(1)


def sync(client, locations, dry_run=False, resources=None, last_modified=None):
    resources = tuple(dict.fromkeys(ENDPOINTS if resources is None else resources))
    if any(resource not in ENDPOINTS for resource in resources):
        raise ValueError("Unknown configuration resource")
    if last_modified is not None and (last_modified.tzinfo is None or last_modified.utcoffset() is None):
        raise ValueError("last_modified requires a UTC offset")
    counts = dict.fromkeys(("returned", "rejected", "skipped", "api_errors", "database_errors", "inserted", "updated"), 0)
    logger.info("Configuration lastModified=%s dry_run=%s", last_modified or "full load", dry_run)
    if last_modified is not None and "tip_withholding" in resources:
        logger.info("tip_withholding has no lastModified filter; refreshing its current value")
    for location in locations:
        guid = str(location["toast_guid"])
        for resource in resources:
            synced_at = datetime.now(UTC)
            try:
                payloads = fetch_records(client, resource, guid, last_modified)
            except (requests.RequestException, ValueError, TypeError) as exc:
                counts["api_errors"] += 1
                response = getattr(exc, "response", None)
                logger.error("Configuration %s restaurant=%s API error=%s status=%s; batch not written",
                             resource, guid, type(exc).__name__,
                             response.status_code if response is not None else "unavailable")
                continue
            counts["returned"] += len(payloads)
            rows = []
            seen_ids = set()
            rejected = 0
            for payload in payloads:
                try:
                    row = normalize_record(resource, payload, guid, synced_at)
                    if row["id"] in seen_ids:
                        raise ValueError("Duplicate configuration GUID in response")
                    seen_ids.add(row["id"])
                    rows.append(row)
                except (ValueError, TypeError) as exc:
                    rejected += 1
                    logger.error("Configuration %s restaurant=%s rejected record (%s)", resource, guid, type(exc).__name__)
            counts["rejected"] += rejected
            logger.info("Configuration %s restaurant=%s fetched=%d valid=%d rejected=%d",
                        resource, guid, len(payloads), len(rows), rejected)
            if rejected:
                counts["skipped"] += len(rows)
                logger.error("Configuration %s restaurant=%s batch not written due to invalid records", resource, guid)
                continue
            if dry_run:
                continue
            try:
                written = upsert_rows(rows, resource, TABLE_COLUMNS[resource],
                                      conflict_columns=("restaurant_id",) if resource == "tip_withholding"
                                      else ("restaurant_id", "id"))
            except (DatabaseError, ConnectionError, ValueError, TypeError) as exc:
                counts["database_errors"] += 1
                counts["skipped"] += len(rows)
                logger.error("Configuration %s restaurant=%s database error=%s SQLSTATE=%s; transaction not confirmed",
                             resource, guid, type(exc).__name__, getattr(exc, "pgcode", None))
                continue
            for key in ("inserted", "updated"):
                counts[key] += written[key]
            logger.info("Configuration %s restaurant=%s inserted=%d updated=%d",
                        resource, guid, written["inserted"], written["updated"])
    logger.info("Configuration totals %s", counts)
    return counts
