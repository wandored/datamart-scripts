import re
from datetime import date, datetime
from openpyxl import load_workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.workbook.defined_name import DefinedName
from openpyxl.utils import quote_sheetname

import pandas as pd

from config import Config
from dbconnect import DatabaseConnection
from toast_utils import extract_menu_items, get_access_token, get_response_data


def get_recipe_ingredient(cur, conn, engine) -> pd.DataFrame:
    cur.execute(
        """
        SELECT ri.concept,
            ri.menu_item,
            ri.recipe,
            ri.ingredient
        FROM recipe_ingredients ri
        WHERE ri.concept = 'Steakhouse' and ri.ingredient LIKE 'WINE %'
        ORDER BY ri.ingredient
        """
    )
    query = cur.fetchall()
    recipe_ingredient = pd.DataFrame(
        query,
        columns=["concept", "toast_item_name", "recipe_name", "purchase_item"],
    )

    cur.execute(
        """
        SELECT i.name FROM item i WHERE i.category2 = 'Wine' ORDER BY i.name
        """
    )
    query = cur.fetchall()
    items = pd.DataFrame(query, columns=["purchase_item"])

    # outer merge recipe_ingredient and items
    df = pd.merge(items, recipe_ingredient, how="outer", on="purchase_item")

    return df


def get_restaurant_names(guid_list, cur) -> pd.DataFrame:
    cur.execute(
        """
        SELECT r.name,
            r.toast_guid
        FROM restaurants r
        WHERE r.toast_guid = ANY(%s::uuid[])
        """,
        (guid_list,),
    )
    query = cur.fetchall()
    form_template = pd.DataFrame(
        query,
        columns=["location_name", "location_guid"],
    )

    return form_template


def get_menu_list(api_access_url, token, guid_list):
    df_menus = pd.DataFrame()
    for guid in guid_list:
        extracted_data = []
        url = api_access_url + "/menus/v2/menus"
        headers = {
            "Toast-Restaurant-External-ID": guid,
            "Authorization": f"Bearer {token}",
        }
        menus = get_response_data(url, headers)

        for menu in menus:
            menu_id = menu.get("guid", "")
            if menu_id != "f3e6905c-8e29-4f65-a7dc-55336b1a544d":
                continue
            menu_name = menu.get("name", "")
            for group in menu.get("menuGroups", []):
                extracted_data.extend(
                    extract_menu_items(guid, menu_id, menu_name, group)
                )
        if extracted_data:
            df_extracted = pd.DataFrame(extracted_data)
            df_menus = pd.concat([df_menus, df_extracted], ignore_index=True)

    return df_menus


def extract_correct_pos_name(row):
    code_map = {
        "NYP-Atlanta": "A",
        "NYP-Boca Raton": "B",
        "NYP-Myrtle Beach": "M",
        "Chophouse 47": "G",
        "Chophouse-NOLA": "L",
        "BT Prime Rib": "T",
    }

    try:
        location_code = row["location_name"] if "location_name" in row else None
    except (ValueError, TypeError):
        return None

    pos_name = row["pos_name"] if "pos_name" in row else ""

    if (
        not location_code
        or not pos_name
        or not isinstance(pos_name, str)
        or pos_name.strip().lower() == "nan"
    ):
        return None

    prefix = code_map.get(location_code)
    if not prefix:
        return None

    # Adjust regex to match codes with the correct prefix
    regex = rf"\b{prefix}\d{{3,4}}\b"
    match = re.search(regex, pos_name)
    if match:
        return match.group(0)

    return None


def main():
    # Authenticate and get access token
    guid_list = Config.LOCATION_GUID_LIST
    api_access_url = "https://ws-api.toasttab.com"
    token = get_access_token(api_access_url)
    if not isinstance(token, dict):
        raise TypeError("Expected token to be a dictionary")
    access_token = token.get("accessToken", "")

    wine_menus = get_menu_list(api_access_url, access_token, guid_list)

    with DatabaseConnection() as db:
        # TODO import list of wines from each location.
        steakhouse_names = get_restaurant_names(guid_list, db.cur)
        steakhouse_wines = pd.merge(
            wine_menus,
            steakhouse_names,
            how="left",
            on="location_guid",
        )
        # Split out bin numbers
        steakhouse_bins = steakhouse_wines.copy()
        steakhouse_bins["bin_number"] = steakhouse_wines.apply(
            extract_correct_pos_name, axis=1
        )
        recipe_ingredients = get_recipe_ingredient(db.cur, db.conn, db.engine)
        df = pd.merge(
            recipe_ingredients, steakhouse_bins, how="outer", on="toast_item_name"
        )
        # create_worksheets(df)
        # TODO merge with store list of changes
        # TODO get most recent purchase from database
        # TODO merge with df
        # TODO create worksheets

        df_pivot = df.pivot_table(
            index=["purchase_item", "recipe_name", "toast_item_name"],
            columns="location_name",
            values="bin_number",
            aggfunc="first",
        )
        today_date = date.today().strftime("%Y-%m-%d")
        df_pivot.to_csv("./output/steakhouse_wine_links.csv", index=True)
        df_pivot.to_excel(
            "/home/wandored/Sync/ReportData/Wine_Forms/Steakhouse_wine_links.xlsx",
            index=True,
            sheet_name=today_date,
        )
        print("Saved steakhouse_wine_links")


if __name__ == "__main__":
    main()
