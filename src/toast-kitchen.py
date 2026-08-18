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


def get_item_fulfillments(guid, business_date):
    toast_client = ToastClient()
    url = "/kitchen/v1/export/itemFulfillments"
    query = {"businessDate": business_date}

    response = toast_client.get_response_data(url, guid, params=query)

    fulfillments = pd.DataFrame(
        [
            {
                "location_guid": row["restaurantGuid"],
                "order_guid": row["orderGuid"],
                "selection_guid": row["selectionGuid"],
                "prep_station_guid": row["prepStationGuid"],
                "fulfill_level": row["itemFulfillmentLevel"],
                "menu_item_guid": row["selectionMenuItemGuid"],
                "kitchen_display": row["prepStationName"],
                "dining_option": row["diningOptionName"],
                "course": row["courseName"],
                "order_source": row["orderSource"],
                "fire_time": row["ticketFiredAt"],
                "start_time": row["itemStartedAt"],
                "fulfill_time": row["itemFulfilledAt"],
            }
            for row in response
        ]
    )
    # Convert timestamps to datetime
    fulfillments["fire_time"] = pd.to_datetime(fulfillments["fire_time"], utc=True)
    fulfillments["fulfill_time"] = pd.to_datetime(
        fulfillments["fulfill_time"], utc=True
    )
    fulfillments.loc[
        fulfillments["fulfill_level"] == 1,
        "kitchen_display",
    ] = "Control"

    fulfillments.loc[
        fulfillments["fulfill_level"] == 2,
        "kitchen_display",
    ] = "Expo"

    fulfillments = fulfillments.sort_values(
        ["location_guid", "selection_guid", "fulfill_level"]
    ).drop_duplicates(
        subset=[
            "location_guid",
            "selection_guid",
            "fulfill_level",
        ],
        keep="last",
    )

    level_columns = {
        0: "prep_station_seconds",
        1: "control_seconds",
        2: "expo_seconds",
    }

    def calculate_timing_chain(group):
        group = group.sort_values("fulfill_level")

        original_fire_time = group.iloc[0]["fire_time"]
        previous_fulfill_time = original_fire_time

        timings = {
            "prep_station_seconds": None,
            "control_seconds": None,
            "expo_seconds": None,
            "total_seconds": None,
        }

        for _, row in group.iterrows():
            level = row["fulfill_level"]

            duration = (row["fulfill_time"] - previous_fulfill_time).total_seconds()

            timings[level_columns[level]] = round(duration)

            previous_fulfill_time = row["fulfill_time"]

        timings["total_seconds"] = round(
            (previous_fulfill_time - original_fire_time).total_seconds()
        )

        return pd.Series(timings)

    timings = (
        fulfillments.groupby(
            [
                "location_guid",
                "selection_guid",
            ],
            group_keys=False,
        )
        .apply(calculate_timing_chain)
        .reset_index()
    )
    stage_names = (
        fulfillments.sort_values("fulfill_level")
        .groupby(
            ["location_guid", "selection_guid"],
            as_index=False,
        )
        .agg(
            first_fulfillment_stage=("kitchen_display", "first"),
        )
    )
    # prep_stations = (
    #     fulfillments[
    #         [
    #             "location_guid",
    #             "selection_guid",
    #             "fulfill_level",
    #             "kitchen_display",
    #         ]
    #     ]
    #     .pivot(
    #         index=["location_guid", "selection_guid"],
    #         columns="fulfill_level",
    #         values="kitchen_display",
    #     )
    #     .rename(columns={0: "prep_station", 1: "control", 2: "expo"})
    #     .reset_index()
    # )

    timings = timings.merge(
        stage_names,
        on=["location_guid", "selection_guid"],
        how="left",
    )
    item_info = fulfillments[
        [
            "location_guid",
            "selection_guid",
            "order_guid",
            "menu_item_guid",
            "dining_option",
            "course",
            "order_source",
        ]
    ].drop_duplicates(subset=["location_guid", "selection_guid"])
    timing_df = item_info.merge(
        timings,
        on=["location_guid", "selection_guid"],
        how="left",
    )
    timing_df["business_date"] = pd.to_datetime(
        business_date,
        format="%Y%m%d",
    ).date()

    timing_df = timing_df[
        [
            "business_date",
            "location_guid",
            "order_guid",
            "selection_guid",
            "menu_item_guid",
            "first_fulfillment_stage",
            "dining_option",
            "course",
            "order_source",
            "prep_station_seconds",
            "control_seconds",
            "expo_seconds",
            "total_seconds",
        ]
    ]
    timing_columns = [
        "prep_station_seconds",
        "control_seconds",
        "expo_seconds",
        "total_seconds",
    ]

    timing_df[timing_columns] = timing_df[timing_columns].astype("Int64")

    timing_df.info()
    timing_df.to_csv("./output/prep_timing.csv", index=False)

    return timing_df


def get_prep_stations(guid, business_date):
    toast_client = ToastClient()
    url = "/kitchen/v1/published/prepStations"
    query = {"lastModified": f"{business_date}T00:00:00.000+0000"}

    response = toast_client.get_response_data(url, guid, params=query)

    prep_stations_df = pd.json_normalize(response)
    print(prep_stations_df)


def write_to_datamart(df):
    with DatabaseConnection() as db:
        columns = [
            "business_date",
            "location_guid",
            "order_guid",
            "selection_guid",
            "menu_item_guid",
            "first_fulfillment_stage",
            "dining_option",
            "course",
            "order_source",
            "prep_station_seconds",
            "control_seconds",
            "expo_seconds",
            "total_seconds",
        ]
        records = [
            tuple(
                None
                if pd.isna(value)
                else value.item()
                if hasattr(value, "item")
                else value
                for value in row
            )
            for row in df[columns].itertuples(index=False, name=None)
        ]

        query = """
            INSERT INTO public.toast_item_timings (
                business_date,
                location_guid,
                order_guid,
                selection_guid,
                menu_item_guid,
                first_fulfillment_stage,
                dining_option,
                course,
                order_source,
                prep_station_seconds,
                control_seconds,
                expo_seconds,
                total_seconds
            )
            VALUES %s
            ON CONFLICT (location_guid, selection_guid)
            DO UPDATE SET
                business_date = EXCLUDED.business_date,
                order_guid = EXCLUDED.order_guid,
                menu_item_guid = EXCLUDED.menu_item_guid,
                first_fulfillment_stage = EXCLUDED.first_fulfillment_stage,
                dining_option = EXCLUDED.dining_option,
                course = EXCLUDED.course,
                order_source = EXCLUDED.order_source,
                prep_station_seconds = EXCLUDED.prep_station_seconds,
                control_seconds = EXCLUDED.control_seconds,
                expo_seconds = EXCLUDED.expo_seconds,
                total_seconds = EXCLUDED.total_seconds;
        """

        db.executemany(query, records)


def main():
    business_date = get_arguments()

    guid = "8d5169ba-1d01-49d5-adb5-a46c341abefe"
    timing_df = get_item_fulfillments(guid, business_date)

    # with DatabaseConnection() as db:
    #     locations = get_locations(db.cur)
    #     for loc in locations:
    #         guid = loc["toast_guid"]
    #         get_item_fulfillments(guid, business_date)
    #         get_prep_stations(guid, business_date)

    write_to_datamart(timing_df)


if __name__ == "__main__":
    main()
