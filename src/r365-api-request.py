from db_utils.r365_utils import R365Client
from db_utils.r365_importers import get_daily_sales
from db_utils.dbconnect import DatabaseConnection


def get_locations():
    with DatabaseConnection() as db:
        db.cur.execute(
            """
            SELECT locationid, name
            FROM restaurants
            WHERE email IS NOT Null
            ORDER BY name
            """
        )
        locations = db.cur.fetchall()

    return locations


if __name__ == "__main__":
    locations = get_locations()
    client = R365Client()

    for location in locations:
        print(location.get("name"), location.get("locationid"))
        location_id = location["locationid"]

        daily_sales = get_daily_sales(client, "2026-07-10", location_id)
        for sale in daily_sales:
            # print name, businessDate, NetSales, guestCount, totalLaborPercentage
            print(
                f"Net Sales: {sale.get('netSales')}, Guest Count: {sale.get('guestCount')}, Total Labor Percentage: {sale.get('totalLaborPercentage')}%"
            )
