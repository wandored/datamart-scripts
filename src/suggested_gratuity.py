"""Enrich a Suggested Gratuity CSV using read-only R365 OData requests.

Run: .venv/bin/python -m src.suggested_gratuity
Optional: --employee-ids-from-db uses previously synced r365.sales_tickets.
Receipt numbers are preserved as supplied by R365; they are not Toast order IDs.
Neither OData sales endpoint exposes a manager ID.
"""

import argparse
import csv
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path
from uuid import UUID

import requests

from db_utils.r365_utils import R365ODataClient


REPORT_COLUMNS = [
    "location_name", "receipt_number", "check_number", "server_name",
    "employee_id", "manager_id", "sales_id", "match_status",
]


def guid(value):
    return str(UUID(str(value)))


def read_input(path, match):
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        fields = reader.fieldnames or []
        required = {"salesdetailid", "menuitem", "date", "location"}
        if not required.issubset(fields):
            raise ValueError(f"CSV missing columns: {', '.join(sorted(required - set(fields)))}")
        rows = []
        skipped = 0
        for number, row in enumerate(reader, start=2):
            if match.casefold() not in (row.get("menuitem") or "").casefold():
                skipped += 1
                continue
            try:
                row["salesdetailid"] = guid(row["salesdetailid"])
                row["location"] = guid(row["location"])
                date.fromisoformat(row["date"][:10])
            except (ValueError, TypeError) as exc:
                raise ValueError(f"Invalid ID or date in CSV row {number}") from exc
            rows.append(row)
    return fields, rows, skipped


def fetch_by_ids(client, entity, id_field, rows, source_id):
    """Use small GUID batches and date windows under R365's 31-day limit."""
    groups = defaultdict(dict)
    for row in rows:
        day = date.fromisoformat(row["date"][:10])
        # 28-day buckets leave room for checks opened before an item was added.
        groups[day.toordinal() // 28][guid(row[source_id])] = day
    records = {}
    for group in groups.values():
        entries = list(group.items())
        # Twenty comparisons exceed this service's 100-node filter limit.
        for offset in range(0, len(entries), 10):
            batch = entries[offset:offset + 10]
            start = min(day for _, day in batch) - timedelta(days=1)
            end = max(day for _, day in batch) + timedelta(days=2)
            ids = " or ".join(f"{id_field} eq {identifier}" for identifier, _ in batch)
            query = (
                f"({ids}) and date ge {start}T00:00:00Z "
                f"and date lt {end}T00:00:00Z"
            )
            wanted = {identifier for identifier, _ in batch}
            for row in client.get_all(entity, params={"$filter": query}):
                identifier = guid(row[id_field])
                if identifier not in wanted:
                    raise ValueError(f"{entity} returned an unrequested ID")
                if identifier in records and records[identifier] != row:
                    raise ValueError(f"{entity} returned conflicting records for one ID")
                records[identifier] = row
        print(f"{entity}: {len(records)} records fetched", flush=True)
    return records


def employee_ids_from_db(sales_ids):
    from db_utils.dbconnect import DatabaseConnection

    if not sales_ids:
        return {}
    with DatabaseConnection() as db:
        db.execute("SET TRANSACTION READ ONLY")
        db.execute(
            """SELECT id, server_id FROM r365.sales_tickets
               WHERE id = ANY(%s::uuid[]) AND is_current IS TRUE""",
            (list(sales_ids),),
        )
        return {str(row["id"]): row["server_id"] for row in db.fetchall()}


def build_report(rows, details, tickets, locations, employees, match):
    report = []
    for source in rows:
        detail = details.get(source["salesdetailid"])
        sales_id = guid(detail["salesID"]) if detail and detail.get("salesID") else ""
        ticket = tickets.get(sales_id)
        status = "matched"
        if detail is None:
            status = "detail_not_found"
        elif match.casefold() not in (detail.get("menuitem") or "").casefold():
            status = "menu_item_mismatch"
        elif guid(detail["location"]) != source["location"]:
            status = "location_mismatch"
        elif ticket is None:
            status = "ticket_not_found"
        elif guid(ticket["location"]) != source["location"]:
            status = "location_mismatch"
        elif source["location"] not in locations:
            status = "location_not_found"
        # Do not attach check information to a mismatched input row.
        header = ticket if status in {"matched", "location_not_found"} else {}
        report.append({
            **source,
            "location_name": locations.get(source["location"], ""),
            "receipt_number": header.get("receiptNumber"),
            "check_number": header.get("checkNumber"),
            "server_name": header.get("server"),
            "employee_id": employees.get(sales_id) if header else None,
            "manager_id": None,
            "sales_id": sales_id,
            "match_status": status,
        })
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("downloads/suggested_gratuity.csv"))
    parser.add_argument("--output", type=Path, default=Path("output/suggested_gratuity_details.csv"))
    parser.add_argument("--match", default="Suggested Gratuity", help="Case-insensitive menu-item substring")
    parser.add_argument(
        "--employee-ids-from-db", action="store_true",
        help="Read server IDs from already synced r365.sales_tickets; unmatched IDs stay blank",
    )
    args = parser.parse_args()
    if args.input.resolve() == args.output.resolve():
        parser.error("Input and output paths must be different")
    fields, rows, skipped = read_input(args.input, args.match)
    print(f"Input: {len(rows)} matching lines; {skipped} nonmatching lines skipped", flush=True)
    details, tickets, locations, employees = {}, {}, {}, {}
    if rows:
        client = R365ODataClient()
        try:
            locations = {
                guid(row["locationId"]): row["name"]
                for row in client.get_all("Location", params={"$select": "locationId,name"})
            }
            details = fetch_by_ids(client, "SalesDetail", "salesdetailID", rows, "salesdetailid")
            tickets = fetch_by_ids(
                client, "SalesEmployee", "salesId",
                [row for row in details.values() if row.get("salesID")], "salesID",
            )
        finally:
            client.session.close()
        if args.employee_ids_from_db:
            employees = employee_ids_from_db(tickets)
    report = build_report(rows, details, tickets, locations, employees, args.match)
    # Write only after every request succeeds; failures must not appear as missing matches.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=REPORT_COLUMNS + [f for f in fields if f not in REPORT_COLUMNS])
        writer.writeheader()
        writer.writerows(report)
    print(f"Wrote {len(report)} line items to {args.output}")
    print(f"Match results: {dict(Counter(row['match_status'] for row in report))}")
    print(f"Distinct matched checks: {len({r['sales_id'] for r in report if r['match_status'] == 'matched'})}")
    print(f"Employee IDs populated: {sum(bool(row['employee_id']) for row in report)}")
    print("receipt_number is R365's POS receipt text, not a verified separate Toast order number.")
    print("Manager IDs are unavailable from these endpoints and remain blank.")


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        # Request exceptions can contain configured URLs; keep diagnostics credential-free.
        status = exc.response.status_code if exc.response is not None else "unavailable"
        raise SystemExit(f"OData request failed ({type(exc).__name__}, HTTP {status}); report not written.") from None
