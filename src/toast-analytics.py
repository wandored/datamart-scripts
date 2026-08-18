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


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Generate analytics from given business dates."
    )
    parser.add_argument(
        "analytics_type",
        choices=["sales", "labor", "check", "menu"],
        help="Type of analytics data to retrieve",
    )
    parser.add_argument(
        "-b",
        "--business_date",
        type=str,
        help="Enter business date in YYYYMMDD format",
    )
    args = parser.parse_args()

    if args.business_date:
        return args
    else:
        args.business_date = pd.to_datetime(
            (pd.Timestamp.now() - pd.Timedelta(days=1))
        ).strftime("%Y%m%d")
        return args


def get_sales_report_guid(guid, args):
    toast_client = ToastClient()

    url = "/era/v1/metrics/day"
    query = {"aggregateBy": "DAY", "onlyInactiveRestaurants": "true"}

    payload = {
        "startBusinessDate": args.business_date,
        "endBusinessDate": args.business_date,
        "restaurantIds": ["8d5169ba-1d01-49d5-adb5-a46c341abefe"],
        "excludedRestaurantIds": [],
        "groupBy": ["REVENUE_CENTER"],
    }

    response = toast_client.get_response_data(url, guid, json=payload, params=query)


def main():
    args = get_arguments()

    guid = "8d5169ba-1d01-49d5-adb5-a46c341abefe"

    reportRequestGuid = get_sales_report_guid(guid, args)

    # with DatabaseConnection() as db:
    #     locations = get_locations(db.cur)
    #     for loc in locations:


if __name__ == "__main__":
    main()
