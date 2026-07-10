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
