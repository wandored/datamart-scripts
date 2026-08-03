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
    query = {"businessDate": business_date}

    response = toast_client.get_response_data(url, guid, params=query)

    fulfillment_df = pd.json_normalize(response)
    print(fulfillment_df)


def get_prep_stations(guid, business_date):
    toast_client = ToastClient()
    url = "/kitchen/v1/published/prepStations"
    query = {"lastModified": business_date}

    response = toast_client.get_response_data(url, guid, params=query)

    prep_stations_df = pd.json_normalize(response)
    print(prep_stations_df)


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

    if args.business_date:
        return args.business_date
    else:
        return pd.to_datetime((pd.Timestamp.now() - pd.Timedelta(days=1))).strftime(
            "%Y%m%d"
        )


def main():
    business_date = get_arguments()

    with DatabaseConnection() as db:
        locations = get_locations(db.cur)

    fulfillment_df = pd.DataFrame()
    for loc in locations:
        guid = loc["toast_guid"]
        # location_fulfillment = get_item_fulfillments(guid, business_date)
        get_prep_stations(guid, business_date)


if __name__ == "__main__":
    main()
