"""
Import sales mix and export menu engineering report to excel
"""

import re
import argparse
from datetime import datetime

import pandas as pd
from psycopg2 import sql
from psycopg2.errors import IntegrityError, UniqueViolation

from db_utils.dbconnect import DatabaseConnection


def get_date(year, period, week, db):
    query = """
        SELECT date FROM calendar
        WHERE year = %s AND period = %s AND week = %s
    """
    db.cur.execute(query, (year, period, week))
    result = db.cur.fetchone()
    if result:
        return result[0]
    else:
        raise ValueError(f"No date found for year={year}, period={period}, week={week}")


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Generate fulfillment report for given business dates."
    )
    parser.add_argument(
        "-y",
        "--year",
        type=str,
        help="Enter Year in YYYY format",
    )
    parser.add_argument(
        "-p",
        "--period",
        type=str,
        help="Enter period in PP format",
    )
    parser.add_argument(
        "-w",
        "--week",
        type=str,
        help="Enter week in WW format",
    )
    args = parser.parse_args()

    return args.year, args.period, args.week


def calculate_bread_basket(df, db):
    stores_w_bread = (4, 9, 11, 15, 16, 17)
    df_bread = df[(df["store_id"].isin(stores_w_bread)) & (df["category2"] == "Entree")]
    for store in stores_w_bread:
        db.cur.execute("SELECT name FROM restaurants WHERE id = %s", (store,))
        store_name = db.cur.fetchone()[0]
        bread_str = r"Bread.*Basket"
        try:
            db.cur.execute(
                "SELECT location, recipe_cost FROM recipe_cost WHERE id = %s AND menu_item ~* %s",
                (store, bread_str),
            )
            matching_rows = pd.DataFrame(
                db.cur.fetchall(), columns=["location", "cost"]
            )
            if not matching_rows.empty:
                bb_cost = matching_rows["cost"].iloc[0]
            elif store == 4:
                bb_cost = 0.19
            elif store == 9:
                bb_cost = 0.19
            elif store == 11:
                bb_cost = 0.84
            elif store == 15:
                bb_cost = 0.96
            elif store == 16:
                bb_cost = 0.69
            elif store == 17:
                bb_cost = 0.84

            entree_count = df_bread.loc[df_bread["store_id"] == store, "quantity"].sum()
            new_row = {
                "location": store_name,
                "store_id": store,
                "concept": "Steakhouse",
                "menu_item": "Bread Basket per Entree",
                "quantity": entree_count,
                "menu_price": 0,
                "menu_cost": bb_cost,
                "sales": 0,
                "category1": "Food",
                "category2": "No Charge",
                "category3": "None",
            }
            df = pd.concat([df, pd.DataFrame(new_row, index=[0])], ignore_index=True)
        except Exception as e:
            print(e)
            pass

    return df


def removePreMods(df):
    # Remove toast Pre-Mods from MenuItem strings
    pre_mods = ["Add", "Extra", "Lite", "On Side"]
    post_mods = ["On Side", "Only"]

    # Create regex patterns for pre and post modifications
    pre_pattern = r"^(" + "|".join(pre_mods) + r")\s+"
    post_pattern = r"\s+(" + "|".join(post_mods) + r")$"

    # Apply the regex patterns to the MenuItem column
    df["menu_item"] = df["menu_item"].apply(lambda x: re.sub(pre_pattern, "", x))
    df["menu_item"] = df["menu_item"].apply(lambda x: re.sub(post_pattern, "", x))

    return df


def merge_dataframes(df1, df2):
    df = pd.merge(df1, df2, on=["location", "menu_item"], how="left", sort=False)
    return df


def get_product_mix(db, year, period, week):
    query = """
    SELECT
        pm.store_id,
        c.week_index,
        c.period_index,
        pm.item_name,
        pm.concept,
        pm.menu_item_id,
        sum(pm.qty_sold) AS quantity,
        avg(pm.menu_item_price) AS menu_price,
        sum(pm.net_item_amt) AS sales,
        m.category_1,
        m.category_2,
        m.category_3,
        rc.recipe_cost AS menu_cost
    FROM toast_product_mix pm
    JOIN menu_items m
        ON m.menu_item_id = pm.menu_item_id
    JOIN calendar c
        ON c.date = pm.date
    JOIN restaurants r
        ON r.id = pm.store_id
    LEFT JOIN weekly_recipe_cost rc
        ON rc.menu_item_id = m.menu_item_id
        AND rc.store_id = pm.store_id
        AND rc.year = c.year
        AND rc.period = c.period
        AND rc.week = c.week
    WHERE c.year = %s
        AND c.period = %s
        AND c.week = %s
    GROUP BY
        pm.store_id,
        c.week_index,
        c.period_index,
        pm.item_name,
        pm.concept,
        pm.menu_item_id,
        m.category_1,
        m.category_2,
        m.category_3,
        rc.recipe_cost
    """
    db.cur.execute(query, (year, period, week))
    result = db.cur.fetchall()
    product_mix = pd.DataFrame(
        result,
        columns=[
            "store_id",
            "week_index",
            "period_index",
            "menu_item",
            "concept",
            "menu_item_id",
            "quantity",
            "menu_price",
            "sales",
            "category1",
            "category2",
            "category3",
            "menu_cost",
        ],
    )

    return product_mix


def main():
    year, period, week = get_arguments()

    with DatabaseConnection() as db:
        product_mix = get_product_mix(db, year, period, week)
        print(product_mix)
        menu_engineering = calculate_bread_basket(product_mix, db)

    menu_engineering["menu_cost"] = menu_engineering["menu_cost"].fillna(0)
    menu_engineering["cost_pct"] = menu_engineering.apply(
        lambda row: row.menu_cost / float(row.menu_price) if row.menu_price else 0,
        axis=1,
    )
    menu_engineering["margin"] = menu_engineering.apply(
        lambda row: float(row.menu_price) - row.menu_cost, axis=1
    )
    menu_engineering["total_cost"] = menu_engineering.apply(
        lambda row: row.quantity * row.menu_cost, axis=1
    )
    menu_engineering["profit"] = menu_engineering.apply(
        lambda row: row.quantity * row.margin, axis=1
    )
    menu_engineering = menu_engineering.reindex(
        columns=[
            "store_id",
            "week_index",
            "period_index",
            "menu_item",
            "concept",
            "menu_item_id",
            "quantity",
            "menu_price",
            "menu_cost",
            "margin",
            "cost_pct",
            "sales",
            "total_cost",
            "profit",
            "category1",
            "category2",
            "category3",
        ]
    )
    # menu_engineering = engineer(menu_engineering)
    # menu_engineering["rating"] = menu_engineering.apply(rating, axis=1)
    # Print rows with null values (up to 50 for readability)
    if hasattr(menu_engineering, "isnull") and hasattr(menu_engineering, "any"):
        null_rows = menu_engineering[menu_engineering.isnull().any(axis=1)]
        if not null_rows.empty:
            null_rows.info()

    menu_engineering = menu_engineering.dropna()

    print(menu_engineering)
    # table_name = "menu_engineering"
    # temp_table_name = f"temp_{table_name}"
    # try:
    #     menu_engineering.to_sql(
    #         temp_table_name,
    #         engine,
    #         if_exists="replace",
    #         index=False,
    #         method="multi",
    #         chunksize=1000,
    #     )
    #     update_query = sql.SQL(
    #         """
    #             INSERT INTO {table} (location, store_id, date, year, period, concept, menu_item, quantity, menu_price, menu_cost, margin, cost_pct, sales, total_cost, profit, category1, category2, category3)
    #             SELECT t.location, t.store_id::integer, t.date, t.year::integer, t.period::integer, t.concept, t.menu_item, t.quantity, t.menu_price, t.menu_cost, t.margin, t.cost_pct, t.sales, t.total_cost, t.profit, t.category1, t.category2, t.category3
    #             FROM {temp_table} AS t
    #             ON CONFLICT (location, store_id, date, Menu_item) DO UPDATE
    #             SET year = EXCLUDED.year,
    #             period = EXCLUDED.period,
    #             concept = EXCLUDED.concept,
    #             quantity = EXCLUDED.quantity,
    #             menu_price = EXCLUDED.menu_price,
    #             menu_cost = EXCLUDED.menu_cost,
    #             margin = EXCLUDED.margin,
    #             cost_pct = EXCLUDED.cost_pct,
    #             sales = EXCLUDED.sales,
    #             total_cost = EXCLUDED.total_cost,
    #             profit = EXCLUDED.profit,
    #             category1 = EXCLUDED.category1,
    #             category2 = EXCLUDED.category2,
    #             category3 = EXCLUDED.category3
    #             """
    #     ).format(
    #         table=sql.Identifier(table_name),
    #         temp_table=sql.Identifier(temp_table_name),
    #     )
    #     db.cur.execute(update_query)
    #     conn.commit()
    # except (IntegrityError, UniqueViolation) as e:
    #     print(e)
    #     return 1
    # except Exception as e:
    #     print(e)
    #     return 1
    # finally:
    #     try:
    #         db.cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
    #         conn.commit()
    #     except Exception as e:
    #         print(e)
    #         conn.rollback()
    # return 0


if __name__ == "__main__":
    main()
