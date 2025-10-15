"""
Track purchases of selected items and create spreadsheet
"""

import pandas as pd

from db_utils.dbconnect import DatabaseConnection


def get_file_path():
    file_path = "./downloads/Receiving by Purchased Item.csv"
    return file_path


def get_table(file_path, cur):
    df = pd.read_csv(
        file_path,
        skiprows=3,
        usecols=[
            "ItemName",
            "LocationName",
            "TransactionNumber",
            "VendorName",
            "VendorItemNumber",
            "TransactionDate",
            "PurchaseUnit",
            "Quantity",
            "AmountEach",
            "ExtPrice2",
        ],
    )
    try:
        filter = df.Quantity.str.match(r"\((.+)\)")
        df = df[~filter]
    except:
        pass

    item_list = [df.ItemName.unique()]
    item_list = [item for sublist in item_list for item in sublist]
    item_list.sort()

    cur.execute(
        """
            SELECT name, equivalent_qty, equivalent_uofm, measure_type, base_uofm, base_qty
            FROM unitsofmeasure
            """
    )
    query = cur.fetchall()
    units = pd.DataFrame(
        query,
        columns=[
            "Name",
            "EquivalentQty",
            "EquivalentUofM",
            "MeasureType",
            "BaseUofM",
            "BaseQty",
        ],
    )
    df = df.merge(units, left_on="PurchaseUnit", right_on="Name", how="left")
    # remove "(" and ")" from Quantity, AmountEach, and ExtPrice2
    df["Quantity"] = df["Quantity"].str.replace("(", "-").str.replace(")", "")
    df["AmountEach"] = df["AmountEach"].str.replace("(", "-").str.replace(")", "")
    df["ExtPrice2"] = df["ExtPrice2"].str.replace("(", "-").str.replace(")", "")

    df["Quantity"] = df["Quantity"].astype(float)
    # df["BaseQty"] = df["BaseQty"].str.replace(",", "").astype(float)
    df["AmountEach"] = df["AmountEach"].str.replace(",", "").astype(float)
    df["ExtPrice2"] = df["ExtPrice2"].astype(str).str.replace(",", "").astype(float)
    # rename df["ExtPrice2"] to df["ExtPrice"] to match other reports
    df.rename(columns={"ExtPrice2": "ExtCost"}, inplace=True)
    # df.loc["Totals"] = df.sum(numeric_only=True)
    sorted_units = (
        df.groupby(["Name"])
        .mean(numeric_only=True)
        .sort_values(by=["Quantity"], ascending=False)
        .reset_index()
    )
    df_sorted = pd.DataFrame()
    for item in item_list:
        df_temp = df[df["ItemName"] == item].copy()
        sorted_units = (
            df_temp.groupby(["Name"])
            .mean(numeric_only=True)
            .sort_values(by=["Quantity"], ascending=False)
            .reset_index()
        )
        report_unit = df_temp.iloc[0]["Name"]
        base_factor = df_temp.iloc[0]["BaseQty"]
        df_temp.loc[:, "reportUnit"] = report_unit
        df_temp.loc[:, "base_factor"] = base_factor
        df_temp.loc[:, "totalQuantity"] = df["Quantity"] * df["BaseQty"] / base_factor
        df_temp.loc[:, "unit"] = report_unit
        df_sorted = pd.concat([df_sorted, df_temp], ignore_index=True)

    return df_sorted


def make_pivot(table):
    vendor = pd.pivot_table(
        table,
        values=["totalQuantity", "ExtCost"],
        index=["ItemName", "VendorName", "unit"],
        aggfunc="sum",
    )
    vendor = (
        vendor.reset_index()
        .sort_values(["ItemName", "VendorName"])
        .set_index("VendorName")
    )
    vendor.loc["Totals"] = vendor.sum(numeric_only=True)
    vendor["CostPerUnit"] = vendor["ExtCost"] / vendor["totalQuantity"]

    restaurant = pd.pivot_table(
        table,
        values=["totalQuantity", "ExtCost"],
        index=["ItemName", "LocationName", "unit"],
        aggfunc="sum",
    )
    restaurant = (
        restaurant.reset_index()
        .sort_values(["ItemName", "LocationName"])
        .set_index("LocationName")
    )
    restaurant.loc["Totals"] = restaurant.sum(numeric_only=True)
    restaurant["CostPerUnit"] = restaurant["ExtCost"] / restaurant["totalQuantity"]
    restaurant.style.format(
        {
            "ExtCost": "${:,.2f}",
            "totalQuantity": "{:,.0f}",
            "CostPerUnit": "${:,.2f}",
        }
    )
    return [vendor, restaurant]


def save_file(table):
    filename = "./output/receiving_by_purchased_item.xlsx"
    with pd.ExcelWriter(filename) as writer:
        vendor, restaurant = make_pivot(df_table)
        vendor.to_excel(writer, sheet_name="Vendor")
        restaurant.to_excel(writer, sheet_name="Restaurant")
        table.to_excel(writer, sheet_name="Detail", index=False)


if __name__ == "__main__":
    file = get_file_path()
    with DatabaseConnection() as db:
        df_table = get_table(file, db.cur)

    save_file(df_table)
