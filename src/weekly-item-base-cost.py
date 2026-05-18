"""
Create and populate the last_purchases table from purchases and unitsofmeasure data.
"""

import sys
import os
import argparse
import pandas as pd
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_utils.dbconnect import DatabaseConnection


def get_end_of_week_date(cur, year, period, week):
    query = """
        SELECT date, week_index, period_index
        FROM calendar
        WHERE year = %s AND period = %s AND week = %s AND dow = 7
        LIMIT 1
    """
    cur.execute(query, (year, period, week))
    result = cur.fetchone()
    if result:
        return result["date"], result["week_index"], result["period_index"]
    else:
        raise ValueError(
            f"No end_of_week_date found for year={year}, period={period}, week={week}"
        )


def calculate_current_cost_per_item(cur, location, end_of_week_date):
    # calculate the base_cost per item based on the most recent purchase price and unit of measure
    query = """
        WITH latest_purchases AS (
            SELECT
                p.date,
                p.store_id,
                p.item_id,
                i.name,
                p.quantity,
                p.uofm,
                p.amount,
                p.base_uofm,
                p.base_qty,
                ROW_NUMBER() OVER (PARTITION BY p.item_id ORDER BY p.date DESC) AS rn
            FROM purchases_pbi p
            JOIN item i ON p.item_id = i.itemid
            WHERE p.store_id = %s AND p.date <= %s AND p.category1 in ('Food', 'LBW')
        )
        SELECT
            date,
            store_id,
            item_id,
            name AS item,
            quantity,
            uofm,
            amount,
            base_uofm,
            base_qty
        FROM latest_purchases
        WHERE rn = 1
    """
    cur.execute(query, (location, end_of_week_date))
    rows = cur.fetchall()
    if not rows:
        print(f"No purchases found for location {location} up to {end_of_week_date}")
        return pd.DataFrame()  # Return empty DataFrame if no purchases found
    df = pd.DataFrame(rows, columns=rows[0].keys())
    df["base_cost"] = df.apply(
        lambda row: (
            (row["amount"] / row["quantity"]) / row["base_qty"]
            if row["quantity"] > 0 and row["base_qty"] > 0
            else 0
        ),
        axis=1,
    )
    df = df.drop(columns=["quantity", "uofm", "amount", "base_qty"])

    return df


def get_zero_cost_items_from_stock_count(cur, location, item_ids, end_of_week_date):
    item_filter = ""
    params = [location, end_of_week_date]
    if item_ids:
        item_filter = " AND scp.item_id = ANY(%s)"
        params.append(item_ids)
    query = f"""
        SELECT date, store_id, item_id, item, quantity, uofm, amount
        FROM (
            SELECT
                scp.date,
                scp.store_id,
                scp.item_id,
                item.name AS item,
                scp.quantity,
                scp.uofm,
                scp.amount,
                ROW_NUMBER() OVER (PARTITION BY scp.item_id ORDER BY scp.date ASC) AS rn
            FROM stock_count_pbi scp
            JOIN item ON scp.item_id = item.itemid
            WHERE scp.store_id = %s AND scp.date <= %s{item_filter}
        ) ranked
        WHERE rn = 1
    """
    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    if not rows:
        print(
            f"No stock count entries found for location {location} and items {item_ids} up to {end_of_week_date}"
        )
        return pd.DataFrame()  # Return empty DataFrame if no stock count entries found
    df = pd.DataFrame(rows, columns=rows[0].keys())
    # merge with unitsofmeasure to get base_uofm
    uofm_names = df["uofm"].unique().tolist()
    uofm_query = """
        SELECT uofm_id, name AS uofm, equivalent_qty, equivalent_uofm, base_uofm, base_qty
        FROM unitsofmeasure
        WHERE name = ANY(%s)
    """
    cur.execute(uofm_query, (uofm_names,))
    uofm_rows = cur.fetchall()
    if not uofm_rows:
        print(f"No unit of measure entries found for items {item_ids}")
        return df  # Return the original DataFrame if no unit of measure entries found
    uofm_df = pd.DataFrame(uofm_rows, columns=uofm_rows[0].keys())
    df = df.merge(uofm_df, on="uofm", how="left")
    df["base_cost"] = df.apply(
        lambda row: (
            (row["amount"] / row["quantity"]) / row["base_qty"]
            if row["quantity"] > 0 and row["base_qty"] > 0
            else 0
        ),
        axis=1,
    )
    df = df.drop(columns=["quantity", "uofm", "amount", "base_qty"])
    return df


# def get_zero_cost_items_from_stock_count(cur, location, item_ids, end_of_week_date):
#     query = """
#         SELECT date, store_id, item_id, item, quantity, uofm, amount
#         FROM (
#             SELECT
#                 scp.date,
#                 scp.store_id,
#                 scp.item_id,
#                 item.name AS item,
#                 scp.quantity,
#                 scp.uofm,
#                 scp.amount,
#                 ROW_NUMBER() OVER (PARTITION BY scp.item_id ORDER BY scp.date ASC) AS rn
#             FROM stock_count_pbi scp
#             JOIN item ON scp.item_id = item.itemid
#             WHERE scp.store_id = %s AND scp.date <= %s AND scp.item_id = ANY(%s)
#         ) ranked
#         WHERE rn = 1
#     """
#     cur.execute(query, (location, end_of_week_date, item_ids))
#     rows = cur.fetchall()
#     if not rows:
#         print(
#             f"No stock count entries found for location {location} and items {item_ids} up to {end_of_week_date}"
#         )
#         return pd.DataFrame()  # Return empty DataFrame if no stock count entries found
#     df = pd.DataFrame(rows, columns=rows[0].keys())
#     # merge with unitsofmeasure to get base_uofm
#     uofm_names = df["uofm"].unique().tolist()
#     uofm_query = """
#         SELECT uofm_id, name AS uofm, equivalent_qty, equivalent_uofm, base_uofm, base_qty
#         FROM unitsofmeasure
#         WHERE name = ANY(%s)
#     """
#     cur.execute(uofm_query, (uofm_names,))
#     uofm_rows = cur.fetchall()
#     if not uofm_rows:
#         print(f"No unit of measure entries found for items {item_ids}")
#         return df  # Return the original DataFrame if no unit of measure entries found
#     uofm_df = pd.DataFrame(uofm_rows, columns=uofm_rows[0].keys())
#     df = df.merge(uofm_df, on="uofm", how="left")
#     df["base_cost"] = df.apply(
#         lambda row: (
#             (row["amount"] / row["quantity"]) / row["base_qty"]
#             if row["quantity"] > 0 and row["base_qty"] > 0
#             else 0
#         ),
#         axis=1,
#     )
#     df = df.drop(columns=["quantity", "uofm", "amount", "base_qty"])
#     return df


def process_week(db, year, period, week):
    end_of_week_date, week_index, period_index = get_end_of_week_date(
        db.cur, year, period, week
    )
    print(
        f"End of week date for year={year}, period={period}, week={week} is {end_of_week_date}"
    )
    # Get list of locations from restaurants table
    db.cur.execute("SELECT id FROM restaurants WHERE toast_id IS NOT NULL")
    store_id = [row["id"] for row in db.cur.fetchall()]
    weekly_item_base_cost_df = (
        pd.DataFrame()
    )  # Initialize an empty DataFrame to store results
    for id in store_id:
        location_df: pd.DataFrame = calculate_current_cost_per_item(
            db.cur, id, end_of_week_date
        )
        stock_count_df = get_zero_cost_items_from_stock_count(
            db.cur, id, None, end_of_week_date
        )
        if not stock_count_df.empty:
            if location_df.empty:
                location_df = stock_count_df.copy()
            else:
                location_item_ids = location_df["item_id"].tolist()
                location_df = location_df.merge(
                    stock_count_df,
                    on="item_id",
                    how="left",
                    suffixes=("", "_sc"),
                    validate="many_to_one",
                )
                has_stock_count = location_df["base_cost_sc"].notna()
                update_mask = (location_df["base_cost"] == 0) & has_stock_count
                location_df.loc[update_mask, "base_cost"] = location_df.loc[
                    update_mask, "base_cost_sc"
                ]
                location_df.loc[update_mask, "base_uofm"] = location_df.loc[
                    update_mask, "base_uofm_sc"
                ]
                location_df.loc[update_mask, "date"] = location_df.loc[
                    update_mask, "date_sc"
                ]
                location_df.loc[update_mask, "item"] = location_df.loc[
                    update_mask, "item_sc"
                ]
                location_df = location_df.drop(
                    columns=[
                        "date_sc",
                        "store_id_sc",
                        "item_sc",
                        "base_uofm_sc",
                        "base_cost_sc",
                    ],
                    errors="ignore",
                )
                missing_items = stock_count_df[
                    ~stock_count_df["item_id"].isin(location_item_ids)
                ]
                if not missing_items.empty:
                    location_df = pd.concat(
                        [location_df, missing_items], ignore_index=True
                    )
        location_df["store_id"] = id
        location_df["year"] = year
        location_df["period"] = period
        location_df["week"] = week
        location_df["week_index"] = week_index
        location_df["period_index"] = period_index
        weekly_item_base_cost_df = pd.concat(
            [weekly_item_base_cost_df, location_df], ignore_index=True
        )
    # drop rows with missing base_uofm
    weekly_item_base_cost_df = weekly_item_base_cost_df.dropna(subset=["base_uofm"])

    # for id in store_id:
    #     location_df: pd.DataFrame = calculate_current_cost_per_item(
    #         db.cur, id, end_of_week_date
    #     )
    #     # for location_df items that have a base_cost of 0, find the most recent cost in stock_count_pbi
    #     zero_cost_items = location_df[location_df["base_cost"] == 0]["item_id"].tolist()
    #     zero_cost_items_df = location_df[location_df["base_cost"] == 0]
    #     if not zero_cost_items_df.empty:
    #         zero_cost_items_df = get_zero_cost_items_from_stock_count(
    #             db.cur, id, zero_cost_items, end_of_week_date
    #         )
    #         print(zero_cost_items_df.head())
    #         if not zero_cost_items_df.empty:
    #             location_df = location_df.merge(
    #                 zero_cost_items_df,
    #                 on="item_id",
    #                 how="left",
    #                 suffixes=("", "_sc"),
    #                 validate="many_to_one",
    #             )
    #             has_stock_count = location_df["base_cost_sc"].notna()
    #             update_mask = (location_df["base_cost"] == 0) & has_stock_count
    #             location_df.loc[update_mask, "base_cost"] = location_df.loc[
    #                 update_mask, "base_cost_sc"
    #             ]
    #             location_df.loc[update_mask, "base_uofm"] = location_df.loc[
    #                 update_mask, "base_uofm_sc"
    #             ]
    #             location_df.loc[update_mask, "date"] = location_df.loc[
    #                 update_mask, "date_sc"
    #             ]
    #             location_df.loc[update_mask, "item"] = location_df.loc[
    #                 update_mask, "item_sc"
    #             ]
    #             location_df = location_df.drop(
    #                 columns=[
    #                     "date_sc",
    #                     "store_id_sc",
    #                     "item_sc",
    #                     "base_uofm_sc",
    #                     "base_cost_sc",
    #                 ],
    #                 errors="ignore",
    #             )
    #         print(location_df[location_df["item_id"].isin(zero_cost_items)].head())
    #
    #     location_df["store_id"] = id
    #     location_df["year"] = year
    #     location_df["period"] = period
    #     location_df["week"] = week
    #     location_df["week_index"] = week_index
    #     location_df["period_index"] = period_index
    #     weekly_item_base_cost_df = pd.concat(
    #         [weekly_item_base_cost_df, location_df], ignore_index=True
    #     )
    # # drop rows with missing base_uofm
    # weekly_item_base_cost_df = weekly_item_base_cost_df.dropna(subset=["base_uofm"])

    # reorder columns
    weekly_item_base_cost_df = weekly_item_base_cost_df[
        [
            "date",
            "year",
            "period",
            "week",
            "week_index",
            "period_index",
            "store_id",
            "item",
            "base_uofm",
            "base_cost",
            "item_id",
        ]
    ]
    # write to csv for debugging
    weekly_item_base_cost_df.to_csv("./output/weekly_item_base_cost.csv", index=False)

    # upsert the date into the weekly_item_base_cost table
    upsert_query = """
        INSERT INTO weekly_item_base_cost (date, year, period, week, week_index, period_index, store_id, item, base_uofm, base_cost, item_id)
        VALUES %s
        ON CONFLICT (week_index, store_id, item) DO UPDATE SET
            date = EXCLUDED.date,
            year = EXCLUDED.year,
            period = EXCLUDED.period,
            week = EXCLUDED.week,
            period_index = EXCLUDED.period_index,
            base_uofm = EXCLUDED.base_uofm,
            base_cost = EXCLUDED.base_cost,
            item_id = EXCLUDED.item_id;
    """
    tuples = [
        (
            row["date"],
            row["year"],
            row["period"],
            row["week"],
            row["week_index"],
            row["period_index"],
            row["store_id"],
            row["item"],
            row["base_uofm"],
            row["base_cost"],
            row["item_id"],
        )
        for _, row in weekly_item_base_cost_df.iterrows()
    ]
    execute_values(db.cur, upsert_query, tuples)
    db.conn.commit()
    print("Data upserted into weekly_item_base_cost table successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Create and populate the last_purchases table."
    )
    parser.add_argument("-y", "--year", type=int, help="Year for the data (e.g., 2024)")
    parser.add_argument("-p", "--period", type=int, help="Period for the data (1-13)")
    parser.add_argument("-w", "--week", type=int, help="Week for the data (1-4)")
    parser.add_argument(
        "--backfill",
        nargs="+",
        type=int,
        metavar="YEAR",
        help="Backfill all weeks for one or more years (e.g., --backfill 2023 2024)",
    )
    args = parser.parse_args()

    with DatabaseConnection() as db:
        if args.backfill:
            query = """
                SELECT DISTINCT year, period, week
                FROM calendar
                WHERE year = ANY(%s)
                ORDER BY year, period, week
            """
            db.cur.execute(query, (args.backfill,))
            weeks = db.cur.fetchall()
            if not weeks:
                raise ValueError(
                    f"No calendar entries found for years: {args.backfill}"
                )

            for row in weeks:
                year, period, week = row["year"], row["period"], row["week"]
                print(f"\nProcessing year={year}, period={period}, week={week}...")
                try:
                    process_week(db, year, period, week)
                except Exception as e:
                    print(f"  ERROR: {e}")
                    continue
        else:
            if not args.year or not args.period or not args.week:
                from datetime import datetime, timedelta

                yesterday = datetime.now() - timedelta(days=1)
                query = """
                    SELECT year, period, week
                    FROM calendar
                    WHERE date = %s
                """
                db.cur.execute(query, (yesterday.date(),))
                result = db.cur.fetchone()
                if result:
                    args.year = result["year"]
                    args.period = result["period"]
                    args.week = result["week"]
                else:
                    raise ValueError(
                        f"No calendar entry found for yesterday's date: {yesterday.date()}"
                    )
            process_week(db, args.year, args.period, args.week)


if __name__ == "__main__":
    main()
