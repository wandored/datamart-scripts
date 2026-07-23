import pandas as pd
import argparse
from db_utils.config import Config
from db_utils.toast_utils import ToastClient
from db_utils.dbconnect import DatabaseConnection


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


def get_item_fulfillments(guid, business_date):
    toast_client = ToastClient()
    url = "/kitchen/v1/export/itemFulfillments"
    query = {"business_date": business_date}

    response = toast_client.get_response_data(url, guid, params=query)

    fulfillment_df = pd.json_normalize(response)
    print(fulfillment_df)


def main():
    parser = argparse.ArgumentParser(
        description="Generate fulfillment report for given business dates."
    )
    parser.add_argument(
        "--business_date",
        type=str,
        help="Enter business date in YYYYMMDD format",
    )
    args = parser.parse_args()

    calendar_date = None
    if args.business_date:
        file_name = "./output/fulfillment_report_" + args.business_date + ".csv"
        business_date = args.business_date
        calendar_date = pd.to_datetime(business_date, format="%Y%m%d").strftime(
            "%Y-%m-%d"
        )
    # if not date provided, default to yesterday and paidBusinessDate
    if not calendar_date:
        calendar_date = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        business_date = pd.to_datetime(calendar_date).strftime("%Y%m%d")

    print(f"Generating report for business date: {calendar_date}")

    with DatabaseConnection() as db:
        locations = get_locations(db.cur)

    fulfillment_df = pd.DataFrame()
    for loc in locations:
        guid = loc["toast_guid"]
        location_fulfillment = get_item_fulfillments(guid, business_date)


if __name__ == "__main__":
    main()
