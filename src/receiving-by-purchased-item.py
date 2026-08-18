"""
Track purchases of selected items and create spreadsheet
"""

import pandas as pd
import numpy as np

from db_utils.dbconnect import DatabaseConnection


def get_file_path():
    file_path = "./downloads/ReceivingbyPurchasedItem.csv"
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
    vendor["CostPerUnit"] = vendor["ExtCost"] / vendor["totalQuantity"].replace(
        0, float("nan")
    )
    vendor = vendor.style.format(
        {
            "ExtCost": "${:,.2f}",
            "totalQuantity": "{:,.0f}",
            "CostPerUnit": "${:,.2f}",
        }
    )

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
    restaurant["CostPerUnit"] = restaurant["ExtCost"] / restaurant[
        "totalQuantity"
    ].replace(0, float("nan"))
    restaurant = restaurant.style.format(
        {
            "ExtCost": "${:,.2f}",
            "totalQuantity": "{:,.0f}",
            "CostPerUnit": "${:,.2f}",
        }
    )

    # product pivot: sum ExtCost and totalQuantity by ItemName x VendorName
    product_sum = pd.pivot_table(
        table,
        values=["ExtCost", "totalQuantity"],
        index="ItemName",
        columns="VendorName",
        aggfunc="sum",
    )

    # product_sum columns are a MultiIndex (first level: ExtCost/totalQuantity, second: vendor names)
    # split into ext cost and qty DataFrames
    if ("ExtCost" in product_sum.columns.levels[0]) and (
        "totalQuantity" in product_sum.columns.levels[0]
    ):
        ext = product_sum["ExtCost"]
        qty = product_sum["totalQuantity"]
    else:
        # defensive fallback if pivot produces different structure
        ext = product_sum.xs("ExtCost", axis=1, level=0, drop_level=False)
        qty = product_sum.xs("totalQuantity", axis=1, level=0, drop_level=False)

    # compute cost per unit (ext cost divided by quantity) per vendor, avoid division by zero
    cost_per_unit = ext.div(qty.replace(0, np.nan))

    # create final product table: vendor columns -> cost per unit, plus TotalQuantity column
    product = cost_per_unit.copy()
    product.columns.name = (
        None  # remove top-level name to have plain vendor column labels
    )
    # total quantity across vendors per ItemName
    total_qty = qty.sum(axis=1)
    product["TotalQuantity"] = total_qty

    # product = pd.pivot_table(
    #     table,
    #     values=["ExtCost", "totalQuantity"],
    #     index="ItemName",
    #     columns="VendorName",
    #     aggfunc="mean",
    # )
    #
    # # Calculate cost per unit for each vendor
    # for vendor_name in product.columns.get_level_values(1).unique():
    #     ext_cost = product[("ExtCost", vendor_name)]
    #     qty = product[("totalQuantity", vendor_name)]
    #     product[("CostPerUnit", vendor_name)] = ext_cost / qty.replace(0, float("nan"))
    #
    # # Add total quantity column
    # product[("totalQuantity", "Total")] = product[("totalQuantity", slice(None))].sum(
    #     axis=1
    # )
    #
    # # Keep only CostPerUnit columns and Total quantity
    # product = product[
    #     [
    #         (col[0], col[1])
    #         for col in product.columns
    #         if col[0] == "CostPerUnit"
    #         or (col[0] == "totalQuantity" and col[1] == "Total")
    #     ]
    # ]

    return [vendor, restaurant, product]


def save_file(table):
    filename = "./output/receiving_by_purchased_item.xlsx"
    with pd.ExcelWriter(filename) as writer:
        vendor, restaurant, product = make_pivot(df_table)
        vendor.to_excel(writer, sheet_name="Vendor")
        restaurant.to_excel(writer, sheet_name="Restaurant")
        product.to_excel(writer, sheet_name="Product")
        table.to_excel(writer, sheet_name="Detail", index=False)


if __name__ == "__main__":
    file = get_file_path()
    with DatabaseConnection() as db:
        df_table = get_table(file, db.cur)

    save_file(df_table)
