"""Add Toast order numbers and action-specific approvers to the R365 report.

Run after src.suggested_gratuity. Reads APIs/database; writes local CSV/cache only.
Toast does not expose an approver for adding/changing a service charge.
"""

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import requests

from db_utils.dbconnect import DatabaseConnection
from db_utils.toast_utils import ToastClient


COLUMNS = [
    "toast_order_number", "toast_check_number", "toast_order_id", "toast_check_id",
    "toast_employee_id", "toast_restaurant_id", "toast_business_date",
    "toast_gratuity_type", "toast_gratuity_ids", "toast_gratuity_amounts",
    "toast_discount_approver_ids", "toast_payment_void_approver_ids",
    "toast_gratuity_approval_status", "toast_match_status",
]


def receipt_date(row):
    # Verified export contains local wall-clock timestamps marked +00. Do not
    # reinterpret these as UTC; use the explicit date in the R365 POS receipt.
    return datetime.strptime(row["receipt_number"].rsplit(" - ", 1)[-1], "%m/%d/%Y").date()


def get_location_mapping(location_ids):
    with DatabaseConnection() as db:
        db.execute("SET TRANSACTION READ ONLY")
        db.execute(
            """SELECT r365_guid, toast_guid FROM core.restaurants
               WHERE r365_guid = ANY(%s::uuid[]) AND toast_guid IS NOT NULL""",
            (list(location_ids),),
        )
        mappings = {}
        for row in db.fetchall():
            key = str(row["r365_guid"])
            value = str(row["toast_guid"])
            if key in mappings and mappings[key] != value:
                raise ValueError("One R365 location maps to multiple Toast restaurants")
            mappings[key] = value
        return mappings


def selections(check):
    def walk(items):
        for item in items or []:
            yield item
            yield from walk(item.get("modifiers"))
    return walk(check.get("selections"))


def reference_id(value):
    return (value or {}).get("guid")


def compact_checks(orders, business_date):
    """Keep only reporting fields; never cache customer or payment details."""
    results = {}
    for order in orders:
        if str(order.get("businessDate")) != business_date.strftime("%Y%m%d"):
            raise ValueError("Toast returned an order for another business date")
        for check in order.get("checks") or []:
            items = list(selections(check))
            gratuities = []
            for kind, values, name_key, amount_key in (
                ("service_charge", check.get("appliedServiceCharges") or [], "name", "chargeAmount"),
                ("selection", items, "displayName", "price"),
            ):
                for value in values:
                    if "suggested gratuity" in (value.get(name_key) or "").casefold():
                        gratuities.append({
                            "id": value["guid"], "type": kind,
                            "amount": str(value[amount_key]) if value.get(amount_key) is not None else None,
                        })
            discounts = list(check.get("appliedDiscounts") or [])
            for item in items:
                discounts.extend(item.get("appliedDiscounts") or [])
            discount_approvers = {
                reference_id(d.get("approver")) for d in discounts
                if reference_id(d.get("approver"))
            }
            void_approvers = {
                reference_id((p.get("voidInfo") or {}).get("voidApprover"))
                for p in check.get("payments") or []
                if reference_id((p.get("voidInfo") or {}).get("voidApprover"))
            }
            result = {
                "order_number": order.get("displayNumber"), "check_number": check.get("displayNumber"),
                "order_id": order["guid"], "check_id": check["guid"],
                "employee_id": reference_id(order.get("server")), "gratuities": gratuities,
                "discount_approvers": sorted(discount_approvers), "payment_void_approvers": sorted(void_approvers),
            }
            key = (order["guid"], check["guid"])
            if key in results and results[key] != result:
                raise ValueError("Toast returned conflicting versions of one check")
            results[key] = result
    return list(results.values())


def match_row(row, candidates, restaurant, business_date):
    result = {**row, **dict.fromkeys(COLUMNS, "")}
    result.update(toast_restaurant_id=restaurant, toast_business_date=business_date.isoformat())
    matching_number = [c for c in candidates if c["check_number"] == row["check_number"]]
    if not matching_number:
        result["toast_match_status"] = "check_not_found"
        return result
    with_gratuity = [c for c in matching_number if c["gratuities"]]
    if not with_gratuity:
        result["toast_match_status"] = "gratuity_not_found"
        return result
    amount = Decimal(row["amount"])
    matches = [c for c in with_gratuity if any(
        g["amount"] is not None and abs(Decimal(g["amount"]) - amount) < Decimal("0.005")
        for g in c["gratuities"]
    )]
    if len(matches) != 1:
        result["toast_match_status"] = "ambiguous_check" if len(matches) > 1 else "gratuity_amount_mismatch"
        return result
    check = matches[0]
    for field in ("order_number", "check_number", "order_id", "check_id", "employee_id"):
        result["toast_" + field] = check[field]
    gratuities = [g for g in check["gratuities"] if g["amount"] is not None and abs(Decimal(g["amount"]) - amount) < Decimal("0.005")]
    result.update(
        toast_gratuity_type=";".join(sorted({g["type"] for g in gratuities})),
        toast_gratuity_ids=";".join(g["id"] for g in gratuities),
        toast_gratuity_amounts=";".join(g["amount"] for g in gratuities),
        toast_discount_approver_ids=";".join(check["discount_approvers"]),
        toast_payment_void_approver_ids=";".join(check["payment_void_approvers"]),
        toast_gratuity_approval_status="not_exposed_by_orders_api",
        toast_match_status="matched",
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("output/suggested_gratuity_details.csv"))
    parser.add_argument("--output", type=Path, default=Path("output/suggested_gratuity_toast.csv"))
    parser.add_argument("--cache-dir", type=Path, default=Path("output/suggested_gratuity_toast_cache"))
    parser.add_argument("--refresh", action="store_true", help="Refresh cached Toast days")
    args = parser.parse_args()
    if args.input.resolve() == args.output.resolve():
        parser.error("Input and output paths must differ")
    with args.input.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames or []
        required = {"location", "receipt_number", "check_number", "amount"}
        if not required.issubset(fields):
            parser.error("Input must be the enriched R365 CSV with location, receipt_number, check_number, and amount")
        rows = list(reader)
    mapping = get_location_mapping({str(UUID(r["location"])) for r in rows}) if rows else {}
    groups = defaultdict(list)
    report = [None] * len(rows)
    for index, row in enumerate(rows):
        location = mapping.get(str(UUID(row["location"])))
        if location is None:
            report[index] = {**row, **dict.fromkeys(COLUMNS, ""), "toast_match_status": "location_not_mapped"}
        else:
            groups[location, receipt_date(row)].append((index, row))
    client = None
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    for number, ((restaurant, day), group) in enumerate(groups.items(), 1):
        cache_path = args.cache_dir / f"{restaurant}_{day}.json"
        if cache_path.exists() and not args.refresh:
            candidates = json.loads(cache_path.read_text())
        else:
            if client is None:
                client = ToastClient(load_locations=False)
            orders = client.get_paged_response_data(
                "/orders/v2/ordersBulk", restaurant,
                params={"businessDate": day.strftime("%Y%m%d")}, parse_float=Decimal,
            )
            candidates = compact_checks(orders, day)
            cache_path.write_text(json.dumps(candidates))
        for index, row in group:
            report[index] = match_row(row, candidates, restaurant, day)
        print(f"Toast day {number}/{len(groups)} ({day}): {sum(report[i]['toast_match_status'] == 'matched' for i, _ in group)}/{len(group)} matched", flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=COLUMNS + [f for f in fields if f not in COLUMNS])
        writer.writeheader()
        writer.writerows(report)
    print(f"Wrote {len(report)} rows to {args.output}")
    print(f"Match results: {dict(Counter(r['toast_match_status'] for r in report))}")
    print("Discount/payment-void approvers are action-specific, not gratuity approvers.")


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        status = exc.response.status_code if exc.response is not None else "unavailable"
        raise SystemExit(f"Toast request failed ({type(exc).__name__}, HTTP {status}); report not written.") from None
