import argparse
from datetime import datetime
from pathlib import Path
import re

import pandas as pd

from db_utils.dbconnect import DatabaseConnection
from db_utils.toast_utils import ToastClient


SELECTION_COLUMNS = [
    "check_number", "item_guid", "item_name", "item_qty", "item_price",
    "created_date", "modified_date",
]


def get_locations(cur, location):
    cur.execute(
        """
        SELECT id, toast_guid
        FROM restaurants
        WHERE name = %s
        """,
        (location,),
    )
    return cur.fetchone()


def selection_frame(orders):
    """Flatten top-level check selections while retaining headers for empty results."""
    rows = []
    for order in orders:
        for check in order.get("checks") or []:
            for selection in check.get("selections") or []:
                rows.append({
                    "check_number": check.get("displayNumber"),
                    "item_guid": selection.get("guid"),
                    "item_name": selection.get("displayName"),
                    "item_qty": selection.get("quantity"),
                    "item_price": selection.get("price"),
                    "created_date": selection.get("createdDate"),
                    "modified_date": selection.get("modifiedDate"),
                })
    return pd.DataFrame(rows, columns=SELECTION_COLUMNS)


def get_order_guid(toast_client, guid, business_date, order_number):
    payload = toast_client.get_response_data(
        "/orders/v2/ordersBulk", guid, params={"businessDate": business_date}
    )
    orders = [
        order for order in payload
        if str(order.get("displayNumber")) == str(order_number)
    ]
    if not orders:
        print(f"No order {order_number} found for business date {business_date}.")
    return selection_frame(orders)


def get_ticket_selections(toast_client, guid, order_guid):
    # This endpoint returns one order. The pagination helper can mistake its
    # nested checks list for a page of results.
    order = toast_client.request(f"/orders/v2/orders/{order_guid}", guid).json()
    return selection_frame([order])


def business_date(value):
    if not re.fullmatch(r"[0-9]{8}", value):
        raise argparse.ArgumentTypeError("Business date must use YYYYMMDD format")
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError:
        raise argparse.ArgumentTypeError("Business date must be a valid YYYYMMDD date") from None
    return value


def get_arguments():
    parser = argparse.ArgumentParser(description="Export Toast check item details for an order.")
    parser.add_argument(
        "--business_date", type=business_date, required=True,
        help="Enter business date in YYYYMMDD format",
    )
    parser.add_argument("--location_name", required=True, help="Enter store name")
    parser.add_argument("--order_number", type=int, required=True, help="Enter Toast order number")
    args = parser.parse_args()
    if not args.location_name.strip():
        parser.error("--location_name must not be blank")
    return args.business_date, args.location_name, args.order_number


def main():
    date, location_name, order_number = get_arguments()
    with DatabaseConnection() as db:
        location = get_locations(db.cur, location_name)
    if location is None:
        raise SystemExit(f"Location not found: {location_name}")
    guid = location["toast_guid"]
    if not guid:
        raise SystemExit(f"Location has no Toast GUID: {location_name}")

    toast_client = ToastClient(load_locations=False)
    frame = get_order_guid(toast_client, guid, date, order_number)
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)
    location_slug = re.sub(r"[^A-Za-z0-9_-]+", "_", location_name).strip("_") or "location"
    output_file = output_dir / f"check_item_details_{location_slug}_{guid}_#{order_number}-{date}.csv"
    frame.to_csv(output_file, index=False)
    print(frame)
    print(f"Saved {len(frame):,} selections to {output_file}")


if __name__ == "__main__":
    main()
