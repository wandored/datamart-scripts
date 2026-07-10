import pandas as pd
import argparse
from db_utils.config import Config
from db_utils.toast_utils import get_access_token, get_response_data
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


def get_payment_identifiers(api_access_url, token, guid, business_date):
    # Return a combined list of the GUIDs for each payment type made during one restaurant business day.
    guid_list = []
    for payment_type in ["paid", "refund", "void"]:
        payload = {f"{payment_type}BusinessDate": business_date}
        url = api_access_url + "/orders/v2/payments"
        headers = {
            "Toast-Restaurant-External-ID": guid,
            "Authorization": f"Bearer {token}",
        }
        resp = get_response_data(url, headers=headers, params=payload)
        if resp:
            guid_list.extend(resp)

    return guid_list


def get_payment_report(api_access_url, token, guid, payment_id_list):
    # For each payment GUID, get the detailed report and combine into a single dataframe.
    all_payments_df = pd.DataFrame()
    for i, payment_id in enumerate(payment_id_list):
        url = api_access_url + f"/orders/v2/payments/{payment_id}"
        headers = {
            "Toast-Restaurant-External-ID": guid,
            "Authorization": f"Bearer {token}",
        }
        resp = get_response_data(url, headers=headers)
        payment_df = pd.json_normalize(resp)
        if i == 0:
            all_payments_df = payment_df
        else:
            all_payments_df = pd.concat(
                [all_payments_df, payment_df], ignore_index=True
            )
    return all_payments_df


def main():
    # add argument parser for payment, refund and void business dates
    parser = argparse.ArgumentParser(
        description="Generate payment report for given business dates."
    )
    parser.add_argument(
        "--payment_date",
        type=str,
        help="Payment business date in YYYYMMDD format",
    )
    args = parser.parse_args()

    calendar_date = None
    if args.payment_date:
        file_name = "./output/payment_report_" + args.payment_date + ".csv"
        business_date = args.payment_date
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

    api_access_url = Config.TOAST_API_ACCESS_URL
    token = get_access_token(api_access_url)
    if not isinstance(token, dict):
        raise TypeError("Expected token to be a dictionary")
    access_token = token.get("accessToken", "")

    master_df = pd.DataFrame()
    for loc in locations:
        guid = loc["toast_guid"]
        payment_id_list = get_payment_identifiers(
            api_access_url, access_token, guid, business_date
        )
        print(f"Location: {loc['name']} - Found {len(payment_id_list)} payments")

        payment_df = get_payment_report(
            api_access_url, access_token, guid, payment_id_list
        )
        payment_df["location_name"] = loc["name"]
        payment_df = payment_df[payment_df["type"] == "CREDIT"]
        payment_df = payment_df[
            ~payment_df["cardEntryMode"].isin(
                [
                    "EMV_CHIP_SIGN",
                    "ONLINE",
                    "SAVED_CARD",
                    "PRE_AUTHED",
                    "FUTURE_ORDER",
                    "INCREMENTAL_PRE_AUTHED",
                ]
            )
        ]
        payment_df = payment_df[
            [
                "guid",
                "originalProcessingFee",
                "isProcessedOffline",
                "type",
                "checkGuid",
                "paidDate",
                "last4Digits",
                "refund",
                "refundStatus",
                "orderGuid",
                "cardEntryMode",
                "paymentStatus",
                "amount",
                "tipAmount",
                "amountTendered",
                "cardType",
                "houseAccount",
                "server.guid",
                "lastModifiedDevice.id",
                "location_name",
            ]
        ]

        if master_df.empty:
            master_df = payment_df
        else:
            master_df = pd.concat([master_df, payment_df], ignore_index=True)

    master_df.to_csv(file_name, index=False)
    print(f"Report saved to {file_name}")

    # Group by location_name and cardEntryMode and sum the amount for each group, then print the results with the count of payments in each group
    summary_df = (
        master_df.groupby(["location_name", "cardEntryMode"])
        .agg(
            total_amount=pd.NamedAgg(column="amount", aggfunc="sum"),
            count=pd.NamedAgg(column="amount", aggfunc="count"),
        )
        .reset_index()
    )
    print("\nSummary by Location and Card Entry Mode:")
    print(summary_df)


if __name__ == "__main__":
    main()
