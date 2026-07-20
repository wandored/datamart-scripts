from db_utils.r365_utils import R365Client
from db_utils.r365_importers import (
    get_daily_sales,
    get_inventory_counts,
    get_inventory_count_by_id,
)
from db_utils.dbconnect import DatabaseConnection
from datetime import datetime, timedelta
import pandas as pd


def get_locations(db):
    db.cur.execute(
        """
        SELECT locationid, name
        FROM restaurants
        WHERE email IS NOT Null
        ORDER BY name
        """
    )
    locations = db.cur.fetchall()

    return locations


def get_calendar(db) -> str:
    # get period, week and year for current_day
    business_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    db.cur.execute(
        """
        SELECT week_index
        FROM calendar
        WHERE date = %s
        """,
        (business_date,),
    )
    calendar = db.cur.fetchone()
    current_week = calendar["week_index"]
    previous_week = current_week - 1

    db.cur.execute(
        """
        SELECT date
        FROM calendar
        WHERE week_index = %s
            AND dow = %s
        """,
        (
            previous_week,
            7,
        ),
    )
    inv_date = db.cur.fetchone()

    return inv_date[0].strftime("%Y-%m-%d")


def print_daily_sales(client, locations):
    business_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    daily_sales_df = pd.DataFrame()
    for location in locations:
        location_id = location["locationid"]
        location_name = location["name"]

        daily_sales = get_daily_sales(client, business_date, location_id)
        if daily_sales:
            location_sales_df = pd.DataFrame(daily_sales)
            location_sales_df["location_name"] = location_name
            location_sales_df = location_sales_df[
                [
                    "location_name",
                    "netSales",
                    "grossSales",
                    "guestCount",
                    "totalLaborHours",
                    "totalLaborAmount",
                    "totalLaborPercentage",
                    "totalDeposit",
                    "expectedCash",
                    "paidInTotal",
                    "paidOutTotal",
                    "overShort",
                ]
            ]
            daily_sales_df = pd.concat(
                [daily_sales_df, location_sales_df], ignore_index=True
            )

    # daily_sales_df = daily_sales_df.drop_duplicates(
    #     subset=["location_name"], keep="last"
    # )
    print(daily_sales_df)


def inventory_count_ids(client, locations, beginning_inventory):
    inventory_counts_df = pd.DataFrame()

    location_inventory = get_inventory_counts(
        client,
        business_date_start=beginning_inventory,
        business_date_end=beginning_inventory,
    )
    if location_inventory:
        location_inventory_df = pd.DataFrame(location_inventory)
        location_inventory_df = location_inventory_df[
            [
                "id",
                "name",
                "status",
                "date",
                "isGLPosting",
                "totalAmount",
                "alertsCount",
            ]
        ]
        inventory_counts_df = pd.concat(
            [inventory_counts_df, location_inventory_df], ignore_index=True
        )

    print(inventory_counts_df)
    inventory_counts_df.to_csv("./output/inventory_counts.csv", index=False)


def inventory_count_by_id(client):
    id = 191245

    inventory_count = get_inventory_count_by_id(client, id)

    inventory_count_df = pd.DataFrame(inventory_count)
    print(inventory_count_df)


def print_menu_items(client, locations):
    business_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    pos_items = {}
    for location in locations:
        menu_items = get_daily_sales(client, business_date, location)
        for menu_item in menu_items:
            tickets = menu_item.get("salesTickets", [])
            for ticket in tickets:
                details = ticket.get("salesDetails", [])
                for detail in details:
                    pos_item = detail.get("posItem")
                    if pos_item:
                        pos_items[pos_item["id"]] = pos_item["name"]

    for pos_item_id, pos_item_name in sorted(pos_items.items(), key=lambda x: x[1]):
        print(pos_item_id, pos_item_name)


if __name__ == "__main__":
    with DatabaseConnection() as db:
        locations = get_locations(db)
        beginning_inventory = get_calendar(db)
        print(f"beginning_inventory date: {beginning_inventory}")
    client = R365Client()

    # print_daily_sales(client, locations)

    # inventory_count_ids(client, locations, beginning_inventory)
    # inventory_count_by_id(client)

    print_menu_items(client, locations)
