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
