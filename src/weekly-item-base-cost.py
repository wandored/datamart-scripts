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
                p.store,
                p.itemid,
                p.item,
                p.quantity,
                p.uofm,
                p.amount,
                uom.base_uofm,
                uom.base_qty,
                ROW_NUMBER() OVER (PARTITION BY p.item ORDER BY p.date DESC) AS rn
            FROM purchases p
            JOIN unitsofmeasure uom ON p.uofm = uom.name
            WHERE p.id = %s AND p.date <= %s AND category1 in ('Food', 'LBW')
        )
        SELECT
            date,
            store,
            itemid,
            item,
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
    df = df.rename(columns={"itemid": "item_id"})

    return df


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
        location_df["store_id"] = id
        location_df["year"] = year
        location_df["period"] = period
        location_df["week"] = week
        location_df["week_index"] = week_index
        location_df["period_index"] = period_index
        weekly_item_base_cost_df = pd.concat(
            [weekly_item_base_cost_df, location_df], ignore_index=True
        )

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


# def update_weekly_recipe_cost(db, year, period, week):
#
#     sql = """
#     INSERT INTO weekly_recipe_cost (
#         store_id,
#         year,
#         period,
#         week,
#         week_index,
#         period_index,
#         recipe_cost,
#         menu_item_id,
#         recipe_id,
#     )
#     SELECT
#         w.store_id,
#         r.recipe_id,
#         r.menu_item_id,
#         w.year,
#         w.period,
#         w.week,
#         w.week_index,
#         w.period_index,
#         SUM(r.qty * w.base_cost) AS recipe_cost
#     FROM weekly_item_base_cost w
#     JOIN restaurants s
#         ON s.id = w.store_id
#     JOIN recipe_ingredients_flat r
#         ON r.item_id = w.item_id
#         AND r.concept = s.concept
#     WHERE
#         w.year = %s
#         AND w.period = %s
#         AND w.week = %s
#     GROUP BY
#         w.store_id,
#         r.recipe_id,
#         r.menu_item_id,
#         w.year,
#         w.period,
#         w.week,
#         w.week_index,
#         w.period_index
#     ON CONFLICT (store_id, concept, menu_item, week_index)
#     DO UPDATE SET recipe_cost = EXCLUDED.recipe_cost;
#     """
#
#     db.cur.execute(sql, (year, period, week))
#     db.conn.commit()


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
        # update_weekly_recipe_cost(db, args.year, args.period, args.week)


if __name__ == "__main__":
    main()
