from requests.exceptions import HTTPError
from urllib.parse import urlparse, parse_qs


# Accounting
def get_invoices(
    client, start_date=None, end_date=None, include_details=False,
    document_date_start=None, document_date_end=None,
):
    """Fetch invoices by modification timestamps or document dates."""
    date_params = {}
    if document_date_start is not None:
        date_params["DateStart"] = str(document_date_start)
    if document_date_end is not None:
        date_params["DateEnd"] = str(document_date_end)
    return client.get_resource(
        "accounting",
        "accounts-payable/invoices",
        collection_key="invoices",
        modifiedOnStart=start_date,
        modifiedOnEnd=end_date,
        IncludeDetails="true" if include_details else "false",
        PageSize=250,
        **date_params,
    )


def get_glaccounts(client, modified_on_start=None, modified_on_end=None):
    return client.get_resource(
        "accounting", "gl-accounts", collection_key="glAccounts",
        ModifiedOnStart=modified_on_start, ModifiedOnEnd=modified_on_end,
    )


def get_pos_mapping(client, col_key, start_date=None):
    return client.get_resource(
        "accounting", "pos-mapping", collection_key=col_key, modifiedOn=start_date
    )


def get_transactions(
    client,
    location_id,
    start_date=None,
    end_date=None,
    business_date_start=None,
    business_date_end=None,
):
    params = {
        "locationId": location_id,
        "modifiedOnStart": start_date,
        "modifiedOnEnd": end_date,
        "pageSize": 250,
    }
    if business_date_start is not None:
        params["dateOfBusinessStart"] = str(business_date_start)
    if business_date_end is not None:
        params["dateOfBusinessEnd"] = str(business_date_end)

    transactions = []

    while True:
        response = client.request(
            "GET",
            "/v1/accounting/transactions",
            params=params,
        )

        transactions.extend(response.get("transactions", []))

        next_link = response.get("nextLink")
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
            response = client.request(
                "GET",
                next_link,
            )

            transactions.extend(response.get("transactions", []))

        break

    return transactions


# Core
def get_locations(client, modified_on_start=None, modified_on_end=None):
    return client.get_resource(
        "core", "locations", collection_key="items",
        modifiedOnStart=modified_on_start, modifiedOnEnd=modified_on_end,
    )


# Inventory
def get_units_of_measure(client, modified_on_start=None, modified_on_end=None):
    return client.get_resource(
        "inventory", "units-of-measure", collection_key="items",
        modifiedOnStart=modified_on_start, modifiedOnEnd=modified_on_end,
    )


def get_item_categories(client, modified_on_start=None, modified_on_end=None):
    return client.get_resource(
        "inventory", "item-categories", collection_key="items",
        modifiedOnStart=modified_on_start, modifiedOnEnd=modified_on_end,
    )


def get_purchase_items(client, modified_on_start=None, modified_on_end=None):
    return client.get_resource(
        "inventory", "items", collection_key="items",
        modifiedOnStart=modified_on_start, modifiedOnEnd=modified_on_end,
    )


def get_inventory_counts(
    client,
    business_date_start=None,
    business_date_end=None,
    status=None,
    location_id=None,
    include_data="none",
    page_size=250,
    modified_on_start=None,
    modified_on_end=None,
):
    return client.get_resource(
        "inventory",
        "inventory-counts",
        dateOfBusinessStart=business_date_start,
        dateOfBusinessEnd=business_date_end,
        modifiedOnStart=modified_on_start, modifiedOnEnd=modified_on_end,
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


def get_vendor_items(
    client, modified_on_start=None, modified_on_end=None, page_size=250
):
    params = {"pageSize": page_size}
    if modified_on_start is not None:
        params["modifiedOnStart"] = modified_on_start
    if modified_on_end is not None:
        params["modifiedOnEnd"] = modified_on_end
    return client.get_resource(
        "inventory",
        "vendor-items",
        collection_key="items",
        **params,
    )


def get_vendor_invoices(
    client,
    modified_on_start=None,
    modified_on_end=None,
    status=None,
    location_id=None,
    include_data="none",
    page_size=50,
    date_start=None,
    date_end=None,
):
    """Fetch inventory invoices with nested details; include_data is legacy."""
    date_params = {}
    if date_start is not None:
        date_params["dateStart"] = str(date_start)
    if date_end is not None:
        date_params["dateEnd"] = str(date_end)
    return client.get_resource(
        "inventory",
        "invoices",
        collection_key="items",
        request_timeout=(15, 180),
        modifiedOnStart=modified_on_start,
        modifiedOnEnd=modified_on_end,
        status=status,
        locationIds=location_id,
        pageSize=page_size,
        **date_params,
    )


# Labor
def get_jobs(client, modified_on_start=None, modified_on_end=None, page_size=250):
    if not modified_on_start and not modified_on_end:
        raise ValueError("Jobs require at least one modified-on date filter")
    return client.get_resource(
        "labor",
        "jobs",
        collection_key="data",
        modifiedOnStart=modified_on_start,
        modifiedOnEnd=modified_on_end,
        pageSize=page_size,
    )


def get_employees(
    client, location_ids, modified_on_start=None, modified_on_end=None, page_size=250
):
    if not location_ids:
        raise ValueError("Employees require at least one location")
    if not modified_on_start and not modified_on_end:
        raise ValueError("Employees require at least one modified-on date filter")
    return client.get_resource(
        "labor",
        "employees",
        collection_key="data",
        locations=",".join(str(location_id) for location_id in location_ids),
        modifiedOnStart=modified_on_start,
        modifiedOnEnd=modified_on_end,
        pageSize=page_size,
    )


# POS


# Sales
def get_daily_sales_pages(client, business_date, location_id):
    """Fetch a complete snapshot; never hide a failed continuation request."""
    try:
        response = client.request(
            "GET", "/v1/sales/daily-sales",
            params={"businessDate": str(business_date), "location": str(location_id),
                    "pageSize": 250},
        )
    except HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            # The API does not distinguish a missing location from a missing DSS.
            # Leave stored data untouched for either case.
            return []
        raise

    pages = []
    followed_links = set()
    while True:
        if not isinstance(response, dict) or not isinstance(response.get("data"), dict):
            raise ValueError("Daily sales response requires a summary object")
        if "nextLink" not in response:
            raise ValueError("Daily sales response is missing pagination metadata")
        pages.append(response["data"])
        next_link = response["nextLink"]
        if next_link is None:
            return pages
        if not isinstance(next_link, str) or not next_link or next_link in followed_links:
            raise ValueError("Invalid or repeated daily sales continuation link")
        followed_links.add(next_link)
        response = client.request("GET", next_link)


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
def get_users(client, page_size=250, modified_on_start=None, modified_on_end=None):
    dates = {}
    if modified_on_start is not None:
        dates["ModifiedOnStart"] = modified_on_start
    if modified_on_end is not None:
        dates["ModifiedOnEnd"] = modified_on_end
    return client.get_resource(
        "user-management", "users", collection_key="entities",
        next_link_key="nextPage", pageSize=page_size,
        **dates,
    )


# Dated labor data. These endpoints have different response envelopes/filters.
def get_labor_punches(client, modified_on_start=None, modified_on_end=None,
                     business_date_start=None, business_date_end=None):
    return client.get_resource(
        "labor", "labor-punches", collection_key="data",
        modifiedOnStart=modified_on_start, modifiedOnEnd=modified_on_end,
        dateOfBusinessStart=str(business_date_start) if business_date_start is not None else None,
        dateOfBusinessEnd=str(business_date_end) if business_date_end is not None else None,
        pageSize=250,
    )


def get_payroll_summaries(client, location_id, start, end):
    # The documented response is a raw array, with no continuation envelope.
    rows = client.request("GET", "/v1/labor/payroll-summaries", params={
        "businessDateStart": str(start), "businessDateEnd": str(end),
        "locationId": str(location_id), "pageSize": 250,
    })
    if not isinstance(rows, list):
        raise ValueError("Payroll summaries requires an array response")
    if len(rows) == 250:
        raise ValueError("Payroll summaries reached pageSize=250 without a continuation link; completeness must be confirmed before writing")
    return rows


def get_sales_analytics(client, location_id, start, end):
    response = client.request("GET", "/v1/labor/sales-analytics", params={
        "locationId": str(location_id), "startDate": str(start), "endDate": str(end),
    })
    if not isinstance(response, dict):
        raise ValueError("Sales analytics requires an object response")
    return response
