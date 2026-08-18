import pandas as pd

from db_utils.dbconnect import DatabaseConnection
from db_utils.toast_utils import ToastClient

rows = []


def get_locations() -> pd.DataFrame:
    locations = pd.DataFrame()
    with DatabaseConnection() as db:
        db.cur.execute(
            """
            SELECT concept, toast_guid
            FROM restaurants
            WHERE email IS NOT Null
            ORDER BY name
            """
        )
        locations = db.cur.fetchall()

    return locations


def walk_groups(groups, concept):
    for group in groups:
        for item in group.get("menuItems", []):
            rows.append(
                {
                    "item_guid": item["guid"],
                    "concept": concept,
                    "multi_location_id": item.get("multiLocationId"),
                    "item_name": item["name"],
                    "sales_category": (item.get("salesCategory") or {}).get("name"),
                }
            )

        walk_groups(group.get("menuGroups", []), concept)


def main():
    locations = get_locations()
    client = ToastClient()
    url = "/menus/v2/menus"

    for loc in locations:
        guid = loc["toast_guid"]
        concept = loc["concept"]

        payload = client.get_response_data(url, guid)

        for menu in payload:
            walk_groups(menu.get("menuGroups", []), concept)

    menu_items_df = (
        pd.DataFrame(rows)
        .drop_duplicates(subset="item_guid")
        .sort_values("item_name")
        .reset_index(drop=True)
    )

    with DatabaseConnection() as db:
        for _, row in menu_items_df.iterrows():
            db.cur.execute(
                """
                INSERT INTO toast_menu_items (
                    item_guid,
                    concept,
                    multi_location_id,
                    item_name,
                    sales_category,
                    last_seen
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (item_guid, concept)
                DO UPDATE SET
                    multi_location_id = EXCLUDED.multi_location_id,
                    item_name = EXCLUDED.item_name,
                    sales_category = EXCLUDED.sales_category,
                    last_seen = NOW()
                """,
                (
                    row["item_guid"],
                    row["concept"],
                    row["multi_location_id"],
                    row["item_name"],
                    row["sales_category"],
                ),
            )

        db.conn.commit()


if __name__ == "__main__":
    main()
