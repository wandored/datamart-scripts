# stockcount_category_totals.py

"""
Scritp to calculate total stock count values per category for a given inventory.
"""

import pandas as pd
import argparse
from db_utils.dbconnect import DatabaseConnection


def get_prep_items(cur, week, period, year):
    cur.execute(
        """
        SELECT 
            sc.store,
            sc.item,
            account,
            uofm,
            sum(sc.quantity) AS count,
            sum(sc.amount) AS amount
        FROM 
            stock_count sc
        WHERE type = 'Stock Count' AND item LIKE 'PREP %%' AND account = 'Food Other' AND week = %s AND period = %s AND year = %s
        GROUP BY 
            sc.store,
            sc.item,
            account,
            uofm
        """,
        (week, period, year),
    )
    query = cur.fetchall()
    df_table = pd.DataFrame(
        query,
        columns=[
            "Store",
            "Item",
            "Account",
            "UofM",
            "Count",
            "Amount",
        ],
    )
    return df_table


def get_purchase_items(cur, week, period, year):
    cur.execute(
        """
        SELECT 
            sc.store,
            sc.item,
            account,
            uofm,
            sum(sc.quantity) AS count,
            sum(sc.amount) AS amount
        FROM 
            stock_count sc
        WHERE type = 'Stock Count' AND category2 = 'Food Other' AND week = %s AND period = %s AND year = %s
        GROUP BY 
            sc.store,
            sc.item,
            account,
            uofm
        """,
        (week, period, year),
    )
    query = cur.fetchall()
    df_table = pd.DataFrame(
        query,
        columns=[
            "Store",
            "Item",
            "Account",
            "UofM",
            "Count",
            "Amount",
        ],
    )
    return df_table


def pivot_table(df):
    pivot = pd.pivot_table(
        df,
        index="Store",
        values="Amount",
        aggfunc="sum",
        margins=True,
        margins_name="Total",
    )
    print(pivot)
    return pivot


def main():
    # add argument parser for week, period and year
    parser = argparse.ArgumentParser(
        description="Generate stock count category totals report."
    )
    parser.add_argument(
        "--week", type=int, required=True, help="Week number for the report"
    )
    parser.add_argument(
        "--period", type=int, required=True, help="Period number for the report"
    )
    parser.add_argument("--year", type=int, required=True, help="Year for the report")
    args = parser.parse_args()

    with DatabaseConnection() as db:
        df_prep = get_prep_items(db.cur, args.week, args.period, args.year)
        pivot_table(df_prep)
        df_purchase = get_purchase_items(db.cur, args.week, args.period, args.year)
        pivot_table(df_purchase)

        # write both files to excel workbook with two sheets
        with pd.ExcelWriter(
            "/home/wandored/Projects/datamart-scripts/output/stockcount_category_totals.xlsx"
        ) as writer:
            df_prep.to_excel(writer, sheet_name="Prep Items", index=False)
            df_purchase.to_excel(writer, sheet_name="Purchase Items", index=False)


if __name__ == "__main__":
    main()
