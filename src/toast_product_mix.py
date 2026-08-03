"""
This script generates a daily product mix report for restaurant locations using Toast POS data.
It retrieves order data, aggregates item sales (including special handling for items with size/price modifiers),
merges with recipe cost data, and enriches with calendar information.
The final report is saved as a CSV and written to a database table, providing detailed sales, pricing,
and cost breakdowns by item and location for a given business date.
The table is designed for power bi to analyze product mix trends, profitability,
and inventory management across locations and time periods.
"""

import re
import pandas as pd
import argparse
import pprint

from pandas._libs.tslibs.timedeltas import disallow_ambiguous_unit
from db_utils.config import Config
from db_utils.dbconnect import DatabaseConnection
from db_utils.toast_utils import ToastClient
from collections import defaultdict, Counter
from datetime import datetime, time
from zoneinfo import ZoneInfo

eastern = ZoneInfo("America/New_York")


def get_locations(cur) -> pd.DataFrame:
    cur.execute(
        """
        SELECT id, name, toast_guid, timezone
        FROM restaurants
        WHERE email IS NOT Null
        ORDER BY name
        """
    )
    locations = cur.fetchall()

    return locations


def format_r365_datetime(date_obj, tz_name, hour=4):
    dt = datetime.combine(date_obj, time(hour=hour), tzinfo=ZoneInfo(tz_name))
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + dt.strftime("%z")


def get_product_mix(client, guid, business_date=None, start_date=None, end_date=None):
    if business_date:
        business_date = pd.to_datetime(business_date).strftime("%Y%m%d")

    url = "/orders/v2/ordersBulk"
    query = {
        "businessDate": business_date,
        "startDate": start_date,
        "endDate": end_date,
    }

    payload = client.get_paged_response_data(url, guid, params=query)

    rows = []
    part_a_names = set()

    def add_row(
        item_guid,
        order_date,
        item_name,
        qty,
        menu,
        gross,
        net=0,
        discount=0,
    ):
        rows.append(
            {
                "item_guid": item_guid,
                "date": order_date,
                "item_name": item_name,
                "qty_sold": qty,
                "menu_item_price": menu,
                "gross_item_amt": gross,
                "net_item_amt": net,
                "discount_amt": discount,
            }
        )

    # Recursive function to process modifiers
    def process_modifiers(modifiers, order_date):
        for mod in modifiers or []:
            mod_item = mod.get("item")
            if not mod_item:
                continue

            add_row(
                item_guid=mod_item["guid"],
                order_date=order_date,
                item_name=mod.get("displayName", "Unknown Modifier"),
                qty=mod.get("quantity", 0) or 0,
                menu=mod.get("receiptLinePrice", 0) or 0,
                gross=mod.get("preDiscountPrice", 0) or 0,
                net=mod.get("price", 0) or 0,
                discount=sum(
                    d.get("discountAmount", 0) or 0
                    for d in mod.get("appliedDiscounts", [])
                ),
            )

            process_modifiers(mod.get("modifiers", []), order_date)

    for order in payload:
        order_date = order["businessDate"]

        for check in order.get("checks", []):
            for sel in check.get("selections", []):
                if sel.get("voided"):
                    continue

                item = sel.get("item")
                if not item:
                    continue

                item_guid = item.get("guid")
                if not item_guid:
                    continue

                item_name = sel.get("displayName", "Unknown Item")
                qty = sel.get("quantity", 0) or 0
                menu = sel.get("receiptLinePrice", 0) or 0
                gross = sel.get("preDiscountPrice", 0) or 0
                net = sel.get("price", 0) or 0
                discount = sum(
                    d.get("discountAmount", 0) or 0
                    for d in sel.get("appliedDiscounts", [])
                )

                # Check if any modifiers have "optionGroupPricingMode" set to "REPLACES_PRICE"
                # This indicates that the item has size/price modifiers that replace the base price.
                has_size_price = any(
                    mod.get("optionGroupPricingMode") == "REPLACES_PRICE"
                    for mod in sel.get("modifiers", [])
                )

                if has_size_price:
                    part_a_names.add(item_name)

                    for mod in sel.get("modifiers", []):
                        if mod.get("price", 0) == 0:
                            continue

                        mod_item = mod.get("item")
                        if not mod_item:
                            continue

                        mod_qty = mod.get("quantity", 0) or 0
                        if mod_qty == 0:
                            continue

                        mod_gross = mod.get("preDiscountPrice", 0) or 0
                        mod_net = mod.get("price", 0) or 0
                        discount = sum(
                            d.get("discountAmount", 0) or 0
                            for d in sel.get("appliedDiscounts", [])
                        )

                        add_row(
                            item_guid=mod_item["guid"],
                            order_date=order_date,
                            item_name=f"{item_name} {mod.get('displayName')}",
                            qty=mod_qty,
                            menu=mod_gross / mod_qty if mod_qty else 0,
                            gross=mod_gross,
                            net=mod_net,
                            discount=discount,
                        )

                else:
                    add_row(
                        item_guid=item_guid,
                        order_date=order_date,
                        item_name=item_name,
                        qty=qty,
                        menu=menu,
                        gross=gross,
                        net=net,
                        discount=discount,
                    )

                    process_modifiers(sel.get("modifiers", []), order_date)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = (
            df.groupby(
                ["item_guid", "date", "item_name"],
                as_index=False,
            )
            .agg(
                {
                    "qty_sold": "sum",
                    "menu_item_price": "mean",
                    "gross_item_amt": "sum",
                    "net_item_amt": "sum",
                    "discount_amt": "sum",
                }
            )
            .sort_values("item_name")
        )

    return df, part_a_names


def extract_part_b(item_name, part_a_names):
    for part_a in part_a_names:
        if item_name.startswith(part_a + " "):
            return item_name[len(part_a) :].strip()
    return item_name


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Generate fulfillment report for given business dates."
    )
    parser.add_argument(
        "-b",
        "--business_date",
        type=str,
        help="Enter business date in YYYYMMDD format",
    )
    args = parser.parse_args()

    return args.business_date


def removeSpecial(df):
    """Removes specialty items from the dataframe"""
    try:
        with open("./specialty.txt") as file:
            specialty_patterns = [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        specialty_patterns = []

    # Remove exact matches
    if specialty_patterns:
        df = df[~df.item_name.isin(specialty_patterns)]

    # Combine all regex patterns into one
    regex_patterns = [
        r"^No ",
        r" Only$",
        r" Tax$",
        r"^& ",
        r"^Seat ",
        r"Allergy$",
        r"Outstanding$",
        r"for Salad.*",
        r".*for Steak.*",
        r".*for Sand.*",
        r".*for Taco.*",
        r".*for Cali-Club.*",
        r".*for Edge.*",
        r".*See Server.*",
        r".*Refund.*",
        r".*2 Pens.*",
    ]
    combined_pattern = "|".join(f"(?:{pat})" for pat in regex_patterns)
    df = df[~df.item_name.str.contains(combined_pattern, na=False, regex=True)]

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Build product mix table for given dates"
    )
    parser.add_argument(
        "-b",
        "--business_date",
        type=str,
        help="Enter business date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "-s",
        "--start_date",
        type=str,
        help="Enter business date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "-e",
        "--end_date",
        type=str,
        help="Enter business date in YYYY-MM-DD format",
    )
    args = parser.parse_args()

    if args.business_date:
        business_date = datetime.strptime(args.business_date, "%Y-%m-%d").date()
        start_date = None
        end_date = None
    elif args.start_date and args.end_date:
        business_date = None
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        end_date += pd.Timedelta(days=1)  # Include the end date in the range

    with DatabaseConnection() as db:
        locations = get_locations(db.cur)

    client = ToastClient()

    product_mix = pd.DataFrame()
    for loc in locations:
        if business_date is None:
            tz = loc["timezone"]
            request_start = format_r365_datetime(start_date, tz)
            request_end = format_r365_datetime(end_date, tz)
        else:
            request_start = None
            request_end = None

        guid = loc["toast_guid"]
        df, part_a_names = get_product_mix(
            client,
            guid,
            business_date,
            request_start,
            request_end,
        )

        df["location"] = loc["name"]
        df["store_id"] = loc["id"]

        df["date"] = pd.to_datetime(
            df["date"].astype(str),
            format="%Y%m%d",
        ).dt.strftime("%Y-%m-%d")

        product_mix = pd.concat([product_mix, df], ignore_index=True)

    # Save original item_name
    product_mix["original_item_name"] = product_mix["item_name"]
    # Extract Part B for merging
    product_mix["merge_item_name"] = product_mix["item_name"].apply(
        lambda x: extract_part_b(x, part_a_names)
    )

    # Restore original item_name
    product_mix["item_name"] = product_mix["original_item_name"]
    product_mix.drop(columns=["original_item_name", "merge_item_name"], inplace=True)

    product_mix = removeSpecial(product_mix)
    # write product_mix to csv
    product_mix.to_csv(f"./output/product_mix_{business_date}.csv", index=False)
    # product_mix = product_mix[~product_mix["cost"].isnull()]

    with DatabaseConnection() as db:
        for _, row in product_mix.iterrows():
            db.cur.execute(
                """
                INSERT INTO toast_product_mix (item_guid, date, store_id, item_name, qty_sold, menu_item_price, gross_item_amt, net_item_amt, discount_amt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (item_guid, date, store_id)
                DO UPDATE SET
                    item_name = EXCLUDED.item_name,
                    qty_sold = EXCLUDED.qty_sold,
                    menu_item_price = EXCLUDED.menu_item_price,
                    gross_item_amt = EXCLUDED.gross_item_amt,
                    net_item_amt = EXCLUDED.net_item_amt,
                    discount_amt = EXCLUDED.discount_amt,
                    last_update = NOW()
                """,
                (
                    row["item_guid"],
                    row["date"],
                    row["store_id"],
                    row["item_name"],
                    row["qty_sold"],
                    row["menu_item_price"],
                    row["gross_item_amt"],
                    row["net_item_amt"],
                    row["discount_amt"],
                ),
            )
        db.conn.commit()


if __name__ == "__main__":
    main()
