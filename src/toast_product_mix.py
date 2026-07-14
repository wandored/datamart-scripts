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
from db_utils.config import Config
from db_utils.dbconnect import DatabaseConnection
from db_utils.toast_utils import ToastClient
from collections import defaultdict
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


def get_product_mix(api_access_url, token, guid, business_date):
    toast_client = ToastClient()
    item_counts = defaultdict(lambda: {"count": 0, "item_name": "", "price": 0})

    url = api_access_url + "/orders/v2/ordersBulk"
    query = {"businessDate": business_date}
    headers = {
        "Toast-Restaurant-External-ID": guid,
        "Authorization": f"Bearer {token}",
    }

    payload = toast_client.get_response_data(url, headers, params=query)
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
                price = sel.get("price", 0) or 0
                quantity = sel.get("quantity", 0) or 0

                item_counts[item_guid]["count"] += quantity
                if not item_counts[item_guid]["item_name"]:
                    item_counts[item_guid]["item_name"] = item_name
                    item_counts[item_guid]["price"] = price

                # Check for menuitems that match the pattern
                if item_name.startswith("Private Dining") or item_name.endswith(
                    "Dinner"
                ):
                    for mod in sel.get("modifiers", []):
                        mod_item = mod.get("item", {})
                        mod_guid = mod_item.get("guid")
                        mod_name = mod.get("displayName", "Unknown Modifier")
                        mod_price = mod.get("price", 0) or 0
                        mod_qty = mod.get("quantity", 0) or 0
                        if mod_guid:
                            item_counts[mod_guid]["count"] += mod_qty
                            if not item_counts[mod_guid]["item_name"]:
                                item_counts[mod_guid]["item_name"] = mod_name
                                item_counts[mod_guid]["price"] = mod_price
                # Check for size/price items

                elif item_name in size_price_items or item_name.endswith("Catering"):
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
                            item_counts[mod_guid]["count"] += mod_qty
                            if not item_counts[mod_guid]["item_name"]:
                                # Combine initial item_name and mod_second_name
                                combined_name = f"{item_name} {mod_size_name}"
                                item_counts[mod_guid]["item_name"] = combined_name
                                # Only use the price from the mod_second_name, divided by quantity for per-item price
                                per_item_price = mod_price / mod_qty
                                item_counts[mod_guid]["price"] = per_item_price
                    # drop the base item and only keep the size/price modifier
                    item_counts[item_guid]["count"] -= quantity
                    if item_counts[item_guid]["count"] <= 0:
                        del item_counts[item_guid]

        # sort item_counts by name
    item_counts = dict(sorted(item_counts.items(), key=lambda x: x[1]["item_name"]))

    return dict(item_counts), part_a_names


def get_recipe_costs(cur) -> pd.DataFrame:
    cur.execute(
        """
        SELECT id, location, concept, menu_item, recipe_cost
        FROM recipe_cost
        """
    )
    costs = cur.fetchall()
    costs = pd.DataFrame(
        costs, columns=["id", "location", "concept", "menu_item", "recipe_cost"]
    )

    return costs


def get_calendar(cur, current_day) -> dict:
    # get period, week and year for current_day
    cur.execute(
        """
        SELECT date, period, week, year
        FROM calendar
        WHERE date = %s
        """,
        (current_day,),
    )
    calendar = cur.fetchone()
    return calendar


def extract_part_b(item_name, part_a_names):
    for part_a in part_a_names:
        if item_name.startswith(part_a + " "):
            return item_name[len(part_a) :].strip()
    return item_name


def main():
    # Enter business date or default to yesterday
    business_date = input(
        "Enter business date (YYYYMMDD) or press Enter for yesterday: "
    )
    # Calendar_date is in format YYYY-MM-DD for querying calendar table
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
        recipe_costs = get_recipe_costs(db.cur)
        calendar = get_calendar(db.cur, calendar_date)

    toast_client = ToastClient()
    api_access_url = toast_client.get_api_access_url()
    token = toast_client.get_access_token()
    if not isinstance(token, dict):
        raise TypeError("Expected token to be a dictionary")
    access_token = token.get("accessToken", "")

    product_mix = pd.DataFrame()
    for loc in locations:
        guid = loc["toast_guid"]
        product_dict, part_a_names = get_product_mix(
            api_access_url, access_token, guid, business_date
        )
        # for item_guid, data in product_mix.items():
        #     print(
        #         f"Name: {data['item_name']}, Count: {data['count']}, Price: {data['price']}"
        #     )
        # append location name to each item in product_mix and add to a pandas DataFrame
        df = pd.DataFrame.from_dict(product_dict, orient="index")
        df["location"] = loc["name"]
        df["store_id"] = loc["id"]
        df.reset_index(inplace=True)
        df.rename(columns={"index": "item_guid"}, inplace=True)
        # merge duplicate rows by summing count. All columns must be the same except for count to be merged
        df = df.groupby(
            ["item_guid", "item_name", "price", "store_id"], as_index=False
        ).agg({"count": "sum"})
        product_mix = pd.concat([product_mix, df], ignore_index=True)

    # Save original item_name
    product_mix["original_item_name"] = product_mix["item_name"]
    # Extract Part B for merging
    product_mix["merge_item_name"] = product_mix["item_name"].apply(
        lambda x: extract_part_b(x, part_a_names)
    )
    # merge product_mix with recipe_costs on location and menu_item = item_name to get recipe_cost
    product_mix = product_mix.merge(
        recipe_costs,
        left_on=["store_id", "merge_item_name"],
        right_on=["id", "menu_item"],
        how="left",
    )

    # Restore original item_name
    product_mix["item_name"] = product_mix["original_item_name"]
    product_mix.drop(columns=["original_item_name", "merge_item_name"], inplace=True)

    # drop menu_item column and rename recipe_cost to cost
    product_mix.drop(columns=["menu_item"], inplace=True)
    product_mix.rename(columns={"recipe_cost": "cost"}, inplace=True)
    # merge product_mix with calendar on date to get period, week and year
    product_mix["date"] = calendar[0]
    product_mix["week"] = calendar[2]
    product_mix["period"] = calendar[1]
    product_mix["year"] = calendar[3]

    # write product_mix to csv
    product_mix.to_csv(f"./output/product_mix_{business_date}.csv", index=False)

    # drop rows with null cost
    product_mix = product_mix[~product_mix["cost"].isnull()]

    # write product_mix to database table product_mix with columns date, location, item_name, count, price, cost, week, period and year
    with DatabaseConnection() as db:
        for _, row in product_mix.iterrows():
            db.cur.execute(
                """
                INSERT INTO product_mix (item_guid, date, week, period, year, id, location, item_name, price, count, cost)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (item_guid, date, id)
                DO UPDATE SET
                    week = EXCLUDED.week,
                    period = EXCLUDED.period,
                    year = EXCLUDED.year,
                    location = EXCLUDED.location,
                    item_name = EXCLUDED.item_name,
                    price = EXCLUDED.price,
                    count = EXCLUDED.count,
                    cost = EXCLUDED.cost
                """,
                (
                    row["item_guid"],
                    row["date"],
                    row["week"],
                    row["period"],
                    row["year"],
                    row["id"],
                    row["location"],
                    row["item_name"],
                    row["price"],
                    row["count"],
                    row["cost"],
                ),
            )
        db.conn.commit()


if __name__ == "__main__":
    main()
