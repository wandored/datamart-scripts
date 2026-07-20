def get_locations(client):
    return client.get_resource("core", "locations")


def get_daily_sales(client, business_date, location_id):
    return client.get_resource(
        "sales",
        "daily-sales",
        collection_key="data",
        businessDate=business_date,
        location=location_id,
    )


def get_units_of_measure(client):
    return client.get_resource("inventory", "units-of-measure")


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
