from requests.exceptions import HTTPError
from urllib.parse import urlparse, parse_qs


# Accounting
def get_glaccounts(client):
    return client.get_resource("accounting", "gl-accounts", collection_key="glAccounts")


def get_pos_mapping(client, col_key, start_date=None):
    return client.get_resource(
        "accounting", "pos-mapping", collection_key=col_key, modifiedOn=start_date
    )


def get_transactions(
    client,
    location_id,
    start_date=None,
    end_date=None,
):
    params = {
        "locationId": location_id,
        "modifiedOnStart": start_date,
        "modifiedOnEnd": end_date,
        "pageSize": 250,
    }

    transactions = []

    while True:
        print("REQUEST PARAMS:", params)

        response = client.request(
            "GET",
            "/v1/accounting/transactions",
            params=params,
        )

        transactions.extend(response.get("transactions", []))

        next_link = response.get("nextLink")
        print("NEXT LINK:", next_link)

        if not next_link:
            break

        # Follow R365's continuation URL exactly
        response = client.request(
            "GET",
            next_link,
        )

        transactions.extend(response.get("transactions", []))

        while response.get("nextLink"):
            next_link = response["nextLink"]
            print("NEXT LINK:", next_link)

            response = client.request(
                "GET",
                next_link,
            )

            transactions.extend(response.get("transactions", []))

        break

    return transactions


# def get_transactions(
#     client,
#     location_id,
#     start_date=None,
#     end_date=None,
# ):
#     params = {
#         "locationId": location_id,
#         "modifiedOnStart": start_date,
#         "modifiedOnEnd": end_date,
#         "pageSize": 250,
#     }
#
#     transactions = []
#
#     while True:
#         print("REQUEST PARAMS:", params)
#
#         response = client.request(
#             "GET",
#             "/v1/accounting/transactions",
#             params=params,
#         )
#
#         transactions.extend(response.get("transactions", []))
#
#         next_link = response.get("nextLink")
#         print("NEXT LINK:", next_link)
#
#         if not next_link:
#             break
#         print("NEXT LINK repr:", repr(next_link))
#         print("NEXT LINK type:", type(next_link))
#
#         continuation_token = parse_qs(urlparse(next_link).query).get(
#             "continuationToken", [None]
#         )[0]
#
#         print("TOKEN repr:", repr(continuation_token))
#         print("TOKEN length:", len(continuation_token) if continuation_token else None)
#
#         if not continuation_token:
#             break
#
#         params = {
#             "locationId": location_id,
#             "continuationToken": continuation_token,
#             "pageSize": 250,
#         }
#         parsed = urlparse(next_link)
#
#         print("PARSED QUERY:", repr(parsed.query))
#         print(
#             "PARSED TOKEN:",
#             repr(parse_qs(parsed.query).get("continuationToken", [None])[0]),
#         )
#     return transactions


# Core
def get_locations(client):
    return client.get_resource("core", "locations")


# Inventory
def get_units_of_measure(client):
    return client.get_resource("inventory", "units-of-measure")


def get_item_categories(client):
    return client.get_resource("inventory", "item-categories")


def get_purchase_items(client):
    return client.get_resource("inventory", "items")


def get_inventory_counts(
    client,
    business_date_start=None,
    business_date_end=None,
    status=None,
    location_id=None,
    include_data="none",
    page_size=250,
):
    return client.get_resource(
        "inventory",
        "inventory-counts",
        dateOfBusinessStart=business_date_start,
        dateOfBusinessEnd=business_date_end,
    )


def get_inventory_count_by_id(client, id):
    return client.get_resource("inventory", "inventory-counts", id)


def get_vendors(client, modified_on_start=None, modified_on_end=None):
    return client.get_resource(
        "inventory",
        "vendors",
        modifiedOnStart=modified_on_start,
        modifiedOnEnd=modified_on_end,
    )


def get_vendor_invoices(
    client,
    modified_on_start=None,
    modified_on_end=None,
    status=None,
    location_id=None,
    include_data="none",
    page_size=250,
):
    return client.get_resource(
        "inventory",
        "inventory-counts",
        modifiedOnStart=modified_on_start,
        modifiedOnEnd=modified_on_end,
    )


# Labor
def get_jobs(client, modified_on_start=None, modified_on_end=None):
    return client.get_resource(
        "labor",
        "jobs",
        collection_key="data",
        modifiedOnStart=modified_on_start,
        modifiedOnEnd=modified_on_end,
    )


# POS


# Sales
def get_daily_sales(client, business_date, location_id):
    try:
        return client.get_resource(
            "sales",
            "daily-sales",
            collection_key="data",
            businessDate=business_date,
            location=location_id,
        )
    except HTTPError as e:
        if e.response.status_code == 404:
            return []
        raise


# User-Management
