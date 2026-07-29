"""
import MenuItem_Export.csv and menu_items_export_r365.csv
compare the "Name" column in both files
drop all items from the MenuItem_Export.csv that are not in the menu_items_export_r365.csv
print the new file to a new csv file
"""

import os

import pandas as pd
import argparse
from datetime import datetime, timedelta
from db_utils.dbconnect import DatabaseConnection
from db_utils.config import Config
from db_utils.toast_utils import ToastClient
from db_utils.r365_utils import R365Client
from db_utils.r365_importers import get_daily_sales


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


def clean_data(toast_export, r365_export):
    # remove rows with any value in Category1, Category2 or Category3 columns from r365_export
    unmapped_menu_items = r365_export[
        r365_export[["category1", "category2", "category3"]].isnull().all(axis=1)
    ]

    # compare the Name columns in both files and drop rows from MenuItem_Export that are in unmapped_menu_items
    new_menu_items = toast_export[
        toast_export["name"].isin(unmapped_menu_items["name"])
    ]
    new_menu_items = new_menu_items.sort_values(by=["name"], ascending=[True])
    # remove duplicates from the new file
    new_menu_items = new_menu_items.drop_duplicates("name")
    new_menu_items = new_menu_items[
        ~new_menu_items["name"].isin(pd.read_csv("./specialty.txt", header=None)[0])
    ]
    # drop all items that being with "No " or "Seat " from the new file
    new_menu_items = new_menu_items[~new_menu_items["name"].str.startswith("No ")]
    new_menu_items = new_menu_items[~new_menu_items["name"].str.startswith("Seat ")]
    new_menu_items = new_menu_items[~new_menu_items["name"].str.startswith("& ")]
    new_menu_items = new_menu_items[~new_menu_items["name"].str.startswith("Splash ")]
    new_menu_items = new_menu_items[~new_menu_items["name"].str.endswith(" Allergy")]
    new_menu_items = new_menu_items[~new_menu_items["name"].str.endswith("for Salad")]
    new_menu_items = new_menu_items[~new_menu_items["name"].str.endswith("for Steak")]
    new_menu_items = new_menu_items[~new_menu_items["name"].str.endswith("for Sand")]
    new_menu_items = new_menu_items[~new_menu_items["name"].str.endswith("for Taco")]
    new_menu_items = new_menu_items[~new_menu_items["name"].str.endswith(" Catering")]
    new_menu_items = new_menu_items[
        ~new_menu_items["name"].str.endswith("for Cali-Club")
    ]
    new_menu_items = new_menu_items[~new_menu_items["name"].str.endswith("for Edge")]

    return new_menu_items


def get_toast_menu_item_list():
    client = ToastClient()

    locations = client.get_restaurants()

    # collect all menu guids for each location guid
    toast_menu_items = pd.DataFrame()
    for location_guid in locations:
        url = "/config/v2/menuItems/"
        response = client.get_response_data(url, location_guid)
        location_menu_items = pd.DataFrame(response)
        location_menu_items = location_menu_items[["guid", "name"]]
        location_menu_items.to_csv("./output/toast_menu_items.csv", index=False)

        toast_menu_items = pd.concat(
            [toast_menu_items, location_menu_items], ignore_index=True
        )
    toast_menu_items = toast_menu_items.drop_duplicates("name")
    return toast_menu_items


def get_r365_menu_item_list(current_menu_items, locations, business_date):
    client = R365Client()

    rows = []
    for location in locations:
        location_id = location["locationid"]

        menu_items = get_daily_sales(client, business_date, location_id)

        for menu_item in menu_items:
            for ticket in menu_item.get("salesTickets", []):
                for detail in ticket.get("salesDetails", []):
                    pos_item = detail.get("posItem")
                    if not pos_item:
                        continue

                    rows.append(
                        {
                            "category1": detail.get("menuItemCategory1"),
                            "category2": detail.get("menuItemCategory2"),
                            "category3": detail.get("menuItemCategory3"),
                            "name": pos_item["name"],
                        }
                    )

    r365_items = (
        pd.DataFrame(rows).drop_duplicates().sort_values("name").reset_index(drop=True)
    )

    # Remove restaurant prefix
    r365_items["name"] = (
        r365_items["name"]
        .str.replace(r"^(Casual|Steakhouse)\s*-\s*", "", regex=True)
        .str.strip()
    )

    r365_items = r365_items.drop_duplicates()

    # Remove items already in the menu_items table
    known_names = set(current_menu_items["name"])
    new_items = r365_items[~r365_items["name"].isin(known_names)].reset_index(drop=True)

    return new_items


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


def main():
    business_date = get_arguments()
    # get list of known menu items from datamart
    with DatabaseConnection() as db:
        locations = get_locations(db)
        db.execute("SELECT menu_item_id, menu_item FROM menu_items")
        current_menu_items = pd.DataFrame(db.fetchall(), columns=["id", "name"])

    r365_menu_items_api = get_r365_menu_item_list(
        current_menu_items, locations, business_date
    )
    toast_menu_items_api = get_toast_menu_item_list()

    new_menu_items = clean_data(toast_menu_items_api, r365_menu_items_api)

    # # write the new file to a csv file
    # new_menu_items.to_csv("./output/new_menu_item_export.csv", index=False)

    # clear screen and print the new file
    os.system("cls" if os.name == "nt" else "clear")
    print(new_menu_items.head(25))


if __name__ == "__main__":
    main()
