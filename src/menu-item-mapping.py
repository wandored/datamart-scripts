"""
import MenuItem_Export.csv and menu_items_export_r365.csv
compare the "Name" column in both files
drop all items from the MenuItem_Export.csv that are not in the menu_items_export_r365.csv
print the new file to a new csv file
"""

import os

import pandas as pd
from db_utils.dbconnect import DatabaseConnection
from db_utils.config import Config
from db_utils.toast_utils import get_restaurants, get_access_token, get_response_data
from db_utils.r365_utils import R365Client


# def get_toast_menu_items():
#     # read in the two csv files
#     toast_df = pd.read_csv(
#         "./downloads/MenuItem_Export_toast.csv",
#         usecols=["Name", "Created Date", "Archived"],
#         encoding="ISO-8859-1",
#     )
#     # drop all rows where the Archived column is Yes
#     toast_df = toast_df[toast_df["Archived"] == "No"]
#     # drop the Archived column
#     toast_df = toast_df.drop(columns=["Archived"])
#     # change the Created Date column to datetime
#     toast_df["Created Date"] = pd.to_datetime(toast_df["Created Date"])
#     toast_df = toast_df.sort_values(by=["Created Date"], ascending=[False])
#
#     return toast_df


def get_r365_menu_items():
    r365_df = pd.read_csv(
        "./downloads/MenuItems_R365.csv",
        usecols=["Name", "Category 1", "Category 2", "Category 3"],
        encoding="ISO-8859-1",
    )
    # drop all rows that do not have "Casual" or "Steakhouse" in Name column
    r365_df = r365_df[r365_df["Name"].str.contains("Casual|Steakhouse")]

    # remove the prefix
    r365_df["Name"] = (
        r365_df["Name"].str.replace("Casual - ", "").str.replace("Steakhouse - ", "")
    )
    return r365_df


def clean_data(toast_export, r365_export):
    # remove rows with any value in Category1, Category2 or Category3 columns from r365_export
    unmapped_menu_items = r365_export[
        r365_export[["Category 1", "Category 2", "Category 3"]].isnull().all(axis=1)
    ]

    # items in toast that have never been ordered (are not in R365)
    unordered_menu_items = toast_export[
        ~toast_export["Name"].isin(unmapped_menu_items["Name"])
    ]
    unordered_menu_items = unordered_menu_items.sort_values(
        by=["Name"], ascending=[True]
    )

    # compare the Name columns in both files and drop rows from MenuItem_Export that are in unmapped_menu_items
    new_menu_items = toast_export[
        toast_export["Name"].isin(unmapped_menu_items["Name"])
    ]
    new_menu_items = new_menu_items.sort_values(by=["Name"], ascending=[True])
    # remove duplicates from the new file
    new_menu_items = new_menu_items.drop_duplicates("Name")
    new_menu_items = new_menu_items[
        ~new_menu_items["Name"].isin(pd.read_csv("./specialty.txt", header=None)[0])
    ]
    # drop all items that being with "No " or "Seat " from the new file
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.startswith("No ")]
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.startswith("Seat ")]
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.startswith("& ")]
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.startswith("Splash ")]
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.endswith(" Allergy")]
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.endswith("for Salad")]
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.endswith("for Steak")]
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.endswith("for Sand")]
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.endswith("for Taco")]
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.endswith(" Catering")]
    new_menu_items = new_menu_items[
        ~new_menu_items["Name"].str.endswith("for Cali-Club")
    ]
    new_menu_items = new_menu_items[~new_menu_items["Name"].str.endswith("for Edge")]

    return new_menu_items, unordered_menu_items


def get_toast_menu_item_list():
    guid = Config.TOAST_RESTAURANT_EXTERNAL_ID
    api_access_url = Config.TOAST_API_ACCESS_URL
    token = get_access_token(api_access_url)
    if not isinstance(token, dict):
        raise TypeError("Expected token to be a dictionary")
    access_token = token.get("accessToken", "")

    locations = get_restaurants(api_access_url, access_token)

    # collect all menu guids for each location guid
    toast_menu_items = pd.DataFrame()
    for location_guid in locations:
        url = api_access_url + "/config/v2/menuItems/"
        headers = {
            "Toast-Restaurant-External-ID": location_guid,
            "Authorization": f"Bearer {access_token}",
        }
        response = get_response_data(url, headers=headers)
        location_menu_items = pd.DataFrame(response)
        location_menu_items = location_menu_items[["guid", "name"]]
        location_menu_items.to_csv("./output/toast_menu_items.csv", index=False)
        location_menu_items.rename(columns={"name": "Name"}, inplace=True)

        toast_menu_items = pd.concat(
            [toast_menu_items, location_menu_items], ignore_index=True
        )
    toast_menu_items = toast_menu_items.drop_duplicates("Name")
    return toast_menu_items


def get_r365_menu_item_list():
    client = R365Client()
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


def main():
    # toast_menu_items = get_toast_menu_items()
    toast_menu_items_api = get_toast_menu_item_list()
    r365_menu_items = get_r365_menu_items()
    new_menu_items, unmapped_menu_items = clean_data(
        toast_menu_items_api, r365_menu_items
    )

    # write the new file to a csv file
    new_menu_items.to_csv("./output/new_menu_item_export.csv", index=False)
    # clear screen and print the new file
    # os.system("cls" if os.name == "nt" else "clear")
    print(new_menu_items.head(30))

    # # compare the Name columns in both files and drop rows from unmapped_menu_items that are not in MenuItem_Export
    # open_items = unmapped_menu_items[
    #     ~unmapped_menu_items["Name"].isin(new_menu_items["Name"])
    # ]
    # open_items = open_items.sort_values(by=["Name"])
    # open_items = open_items.drop_duplicates()
    # # write the open_items file to a csv file
    # open_items.to_csv("./output/open_items.csv", index=False)


if __name__ == "__main__":
    main()

    # build list using toast & r365 api
    # toast_menu_items_df = get_toast_menu_item_list()
    # r365_menu_items_df = get_r365_menu_item_list()
