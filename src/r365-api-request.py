from db_utils.r365_utils import R365Client


if __name__ == "__main__":
    client = R365Client()
    locations = client.get_resource("core", "locations")
    for location in locations:
        location_id = location.get("id")

        daily_sales = client.get_resource(
            "sales",
            "daily-sales",
            params={"businessDate": "2026-07-09", "location": location_id},
        )
        for sale in daily_sales:
            print(sale.get("netSales"))
