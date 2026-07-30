"""
This script generates a daily product mix report for restaurant locations using Toast POS data.
It retrieves order data, aggregates item sales (including special handling for items with size/price modifiers),
merges with recipe cost data, and enriches with calendar information.
The final report is saved as a CSV and written to a database table, providing detailed sales, pricing,
and cost breakdowns by item and location for a given business date.
The table is designed for power bi to analyze product mix trends, profitability,
and inventory management across locations and time periods.
"""

import pandas as pd
import argparse
import pprint
from db_utils.config import Config
from db_utils.dbconnect import DatabaseConnection
from db_utils.toast_utils import ToastClient
from collections import defaultdict, Counter
from zoneinfo import ZoneInfo

eastern = ZoneInfo("America/New_York")


def get_locations(cur) -> pd.DataFrame:
    cur.execute(
        """
        SELECT id, name, toast_guid
        FROM restaurants
        WHERE email IS NOT Null
        ORDER BY name
        """
    )
    locations = cur.fetchall()

    return locations


def get_product_mix(client, guid, business_date):
    item_counts = defaultdict(
        lambda: {
            "qty_sold": 0,
            "item_name": "",
            "gross_item_amt": 0,
            "net_item_amt": 0,
            "discount_amt": 0,
        }
    )

    def process_modifiers(modifiers):
        for mod in modifiers:
            mod_item = mod.get("item", {})
            mod_guid = mod_item.get("guid")
            if not mod_guid:
                continue
            mod_name = mod.get("displayName", "Unknown Modifier")
            mod_qty = mod.get("quantity", 0) or 0
            mod_gross = mod.get("price", 0) or 0
            mod_net = mod.get("receiptLinePrice", 0) or 0
            item_counts[mod_guid]["qty_sold"] += mod_qty
            item_counts[mod_guid]["gross_item_amt"] += mod_gross
            item_counts[mod_guid]["net_item_amt"] += mod_net
            # recipe_costs = get_recipe_costs(db.cur)
            # calendar = get_calendar(db.cur, calendar_date)
            if not item_counts[mod_guid]["item_name"]:
                item_counts[mod_guid]["item_name"] = mod_name
            process_modifiers(mod.get("modifiers", []))

    url = "/orders/v2/ordersBulk"
    query = {"businessDate": business_date}

    payload = client.get_paged_response_data(url, guid, params=query)
    size_price_items = ["Live Maine Lobster", "Stone Crab"]
    part_a_names = set()  # Track Part A names for later splitting

    for order in payload:
        checks = order.get("checks", [])

        for check in checks:
            selections = check.get("selections", [])

            for sel in selections:
                if sel.get("voided", False):
                    continue
                item = sel.get("item", {})
                if not item or not item.get("guid"):
                    continue
                item_guid = item["guid"]
                item_name = sel.get("displayName", "Unknown Item")
                quantity = sel.get("quantity", 0) or 0
                gross_price = sel.get("price", 0) or 0
                net_price = sel.get("receiptLinePrice", 0) or 0
                discount = sum(
                    d.get("discountAmount", 0) or 0
                    for d in sel.get("appliedDiscounts", [])
                )

                # if mod.get("appliedDiscounts"):
                #     pprint.pp(mod)

                # Check for size/price items
                if item_name in size_price_items or item_name.endswith("Catering"):
                    part_a_names.add(item_name)
                    for mod in sel.get("modifiers", []):
                        # Only process modifiers that have a price (indicating they are size/price modifiers)
                        if mod.get("price", 0) == 0:
                            continue
                        mod_item = mod.get("item", {})
                        mod_guid = mod_item.get("guid")
                        mod_size_name = mod.get("displayName", "Unknown Modifier")
                        mod_price = mod.get("price", 0) or 0
                        mod_qty = mod.get("quantity", 0) or 0
                        if mod_guid:
                            item_counts[mod_guid]["qty_sold"] += mod_qty
                            # Only use the price from the mod_second_name, divided by quantity for per-item price
                            per_item_price = mod_price / mod_qty
                            item_counts[mod_guid]["gross_item_amt"] += per_item_price
                            if not item_counts[mod_guid]["item_name"]:
                                # Combine initial item_name and mod_second_name
                                combined_name = f"{item_name} {mod_size_name}"
                                item_counts[mod_guid]["item_name"] = combined_name
                    # drop the base item and only keep the size/price modifier
                    item_counts[item_guid]["qty_sold"] -= quantity
                    if item_counts[item_guid]["qty_sold"] <= 0:
                        del item_counts[item_guid]

                else:
                    item_counts[item_guid]["qty_sold"] += quantity
                    item_counts[item_guid]["gross_item_amt"] += gross_price
                    item_counts[item_guid]["net_item_amt"] += net_price
                    item_counts[item_guid]["discount_amt"] += discount
                    if not item_counts[item_guid]["item_name"]:
                        item_counts[item_guid]["item_name"] = item_name

                    process_modifiers(sel.get("modifiers", []))

    # sort item_counts by name
    item_counts = dict(sorted(item_counts.items(), key=lambda x: x[1]["item_name"]))

    return dict(item_counts), part_a_names


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
        # r"Catering$",
    ]
    combined_pattern = "|".join(f"(?:{pat})" for pat in regex_patterns)
    df = df[~df.item_name.str.contains(combined_pattern, na=False, regex=True)]

    return df


def main():
    # Enter business date or default to yesterday
    business_date = get_arguments()

    # convert calendar format
    calendar_date = None
    if business_date:
        calendar_date = pd.to_datetime(business_date, format="%Y%m%d").strftime(
            "%Y-%m-%d"
        )
    if not business_date:
        adjusted_date = pd.Timestamp.now(tz=eastern).normalize() - pd.Timedelta(days=1)
        business_date = adjusted_date.strftime("%Y%m%d")
        calendar_date = adjusted_date.strftime("%Y-%m-%d")

    with DatabaseConnection() as db:
        locations = get_locations(db.cur)

    client = ToastClient()

    product_mix = pd.DataFrame()
    for loc in locations:
        guid = loc["toast_guid"]
        product_dict, part_a_names = get_product_mix(client, guid, business_date)
        # append location name to each item in product_mix and add to a pandas DataFrame
        df = pd.DataFrame.from_dict(product_dict, orient="index")
        df["location"] = loc["name"]
        df["store_id"] = loc["id"]
        df.reset_index(inplace=True)
        df.rename(columns={"index": "item_guid"}, inplace=True)
        df = df.groupby(["item_guid", "item_name", "store_id"], as_index=False).agg(
            {
                "qty_sold": "sum",
                "gross_item_amt": "sum",
                "net_item_amt": "sum",
                "discount_amt": "sum",
            }
        )
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
    product_mix["date"] = calendar_date
    # write product_mix to csv
    product_mix.to_csv(f"./output/product_mix_{business_date}.csv", index=False)
    # product_mix = product_mix[~product_mix["cost"].isnull()]

    print(product_mix)
    with DatabaseConnection() as db:
        for _, row in product_mix.iterrows():
            db.cur.execute(
                """
                INSERT INTO toast_product_mix (item_guid, date, store_id, item_name, qty_sold, gross_item_amt, net_item_amt, discount_amt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (item_guid, date, store_id)
                DO UPDATE SET
                    item_name = EXCLUDED.item_name,
                    gross_item_amt = EXCLUDED.gross_item_amt,
                    qty_sold = EXCLUDED.qty_sold
                """,
                (
                    row["item_guid"],
                    row["date"],
                    row["store_id"],
                    row["item_name"],
                    row["qty_sold"],
                    row["gross_item_amt"],
                    row["net_item_amt"],
                    row["discount_amt"],
                ),
            )
        db.conn.commit()


if __name__ == "__main__":
    main()
