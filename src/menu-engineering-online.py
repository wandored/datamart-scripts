"""
Import sales mix and export menu engineering report to excel
"""

import re
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


def get_start_date(year, period, week):
    with DatabaseConnection() as db:
        query = """
            SELECT date FROM calendar
            WHERE year = %s AND period = %s AND week = %s
        """
        db.cur.execute(query, (year, period, week))
        result = db.cur.fetchone()
    return result[0]


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


def update_location_names(df, db):
    # import locationid and name from location table
    db.cur.execute("SELECT locationid, name FROM location")
    location = db.cur.fetchall()
    location = pd.DataFrame(location, columns=["locationid", "name"])
    location.rename(columns={"name": "location"}, inplace=True)
    df = pd.merge(df, location, on="location", how="left", sort=False)

    db.cur.execute("SELECT locationid, name, id FROM restaurants")
    restaurants = db.cur.fetchall()
    restaurants = pd.DataFrame(restaurants, columns=["locationid", "name", "id"])
    restaurants.dropna(inplace=True)
    restaurants.rename(columns={"name": "location", "id": "store_id"}, inplace=True)
    df = pd.merge(df, restaurants, on="locationid", how="left", sort=False)

    df.drop(columns=["locationid", "location_x"], inplace=True)
    df.rename(columns={"location_y": "location"}, inplace=True)
    df[["concept", "menu_item"]] = df["menu_item"].str.split(" - ", n=1, expand=True)
    df = df.reindex(
        columns=[
            "location",
            "store_id",
            "concept",
            "menu_item",
            "quantity",
            "menu_price",
            "sales",
            "category1",
            "category2",
            "category3",
            "menu_cost",
        ]
    )

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


def main(product_mix_csv, menu_analysis_csv, date, year, period, db):
    product_mix = pd.read_csv(
        product_mix_csv,
        skiprows=3,
        sep=",",
        thousands=",",
        usecols=[
            "TransferDate",
            "Textbox27",
            "Qty",
            "Cost",
            "Total",
            "Cat1",
            "Cat2",
            "Cat3",
        ],
    )
    product_mix.rename(
        columns={
            "TransferDate": "menu_item",
            "Textbox27": "location",
            "Qty": "quantity",
            "Cost": "menu_price",
            "Total": "sales",
            "Cat1": "category1",
            "Cat2": "category2",
            "Cat3": "category3",
        },
        inplace=True,
    )
    # product_mix = removePreMods(product_mix)

    product_mix.sort_values(by=["menu_item"], inplace=True)
    product_mix["category1"] = product_mix["category1"].fillna("None")
    product_mix["category2"] = product_mix["category2"].fillna("None")
    product_mix["category3"] = product_mix["category3"].fillna("None")

    menu_analysis = pd.read_csv(
        menu_analysis_csv,
        skiprows=3,
        sep=",",
        thousands=",",
        usecols=["Location", "MenuItemName", "UnitCost_Loc"],
    )
    menu_analysis["Location"] = menu_analysis["Location"].str.strip()
    menu_analysis.rename(
        columns={
            "Location": "location",
            "MenuItemName": "menu_item",
            "UnitCost_Loc": "menu_cost",
        },
        inplace=True,
    )
    menu_analysis["menu_cost"] = menu_analysis["menu_cost"].fillna(0)
    df_merge = merge_dataframes(product_mix, menu_analysis)
    menu_engineering = update_location_names(df_merge, db)
    menu_engineering = calculate_bread_basket(menu_engineering, db)

    menu_engineering["date"] = date
    menu_engineering["period"] = period
    menu_engineering["year"] = year
    menu_engineering["menu_cost"] = menu_engineering["menu_cost"].fillna(0)
    menu_engineering["cost_pct"] = menu_engineering.apply(
        lambda row: row.menu_cost / row.menu_price if row.menu_price else 0, axis=1
    )
    menu_engineering["margin"] = menu_engineering.apply(
        lambda row: row.menu_price - row.menu_cost, axis=1
    )
    menu_engineering["total_cost"] = menu_engineering.apply(
        lambda row: row.quantity * row.menu_cost, axis=1
    )
    menu_engineering["profit"] = menu_engineering.apply(
        lambda row: row.quantity * row.margin, axis=1
    )
    menu_engineering = menu_engineering.reindex(
        columns=[
            "location",
            "store_id",
            "date",
            "year",
            "period",
            "concept",
            "menu_item",
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

    table_name = "menu_engineering"
    temp_table_name = f"temp_{table_name}"
    try:
        menu_engineering.to_sql(
            temp_table_name,
            engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )
        update_query = sql.SQL(
            """
                INSERT INTO {table} (location, store_id, date, year, period, concept, menu_item, quantity, menu_price, menu_cost, margin, cost_pct, sales, total_cost, profit, category1, category2, category3)
                SELECT t.location, t.store_id::integer, t.date, t.year::integer, t.period::integer, t.concept, t.menu_item, t.quantity, t.menu_price, t.menu_cost, t.margin, t.cost_pct, t.sales, t.total_cost, t.profit, t.category1, t.category2, t.category3
                FROM {temp_table} AS t
                ON CONFLICT (location, store_id, date, Menu_item) DO UPDATE
                SET year = EXCLUDED.year,
                period = EXCLUDED.period,
                concept = EXCLUDED.concept,
                quantity = EXCLUDED.quantity,
                menu_price = EXCLUDED.menu_price,
                menu_cost = EXCLUDED.menu_cost,
                margin = EXCLUDED.margin,
                cost_pct = EXCLUDED.cost_pct,
                sales = EXCLUDED.sales,
                total_cost = EXCLUDED.total_cost,
                profit = EXCLUDED.profit,
                category1 = EXCLUDED.category1,
                category2 = EXCLUDED.category2,
                category3 = EXCLUDED.category3
                """
        ).format(
            table=sql.Identifier(table_name),
            temp_table=sql.Identifier(temp_table_name),
        )
        db.cur.execute(update_query)
        conn.commit()
    except (IntegrityError, UniqueViolation) as e:
        print(e)
        return 1
    except Exception as e:
        print(e)
        return 1
    finally:
        try:
            db.cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
    return 0


if __name__ == "__main__":
    year, period, week = get_arguments()
    print(f"Year: {year}, Period: {period}, Week: {week}")

    menu_price_analysis = "./downloads/Menu Price Analysis.csv"
    date = get_date(product_mix)
    with DatabaseConnection() as db:
        product_mix = get_product_mix(db)
        period, year = get_period(date, db)
        print(f"Date: {date}, Period: {period}, Year: {year}")
        main(product_mix, menu_price_analysis, date, year, period, db)
