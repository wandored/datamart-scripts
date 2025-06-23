"""
This script reads data from a CSV file (`PurchaseItems.csv`), processes it by
renaming columns for consistency, and then writes the data into a PostgreSQL
database table named `item_conversion`. It uses `pandas` to handle the CSV file,
`sqlalchemy` to establish a connection to the database, and `psycopg2` for
executing SQL queries. The script performs an upsert operation, updating
existing records in the `item_conversion` table or inserting new ones, and
ensures temporary tables are cleaned up after execution.
Error handling is included to manage integrity errors and cleanup failures.
"""

import pandas as pd
from psycopg2 import sql
from psycopg2.errors import IntegrityError

from dbconnect import DatabaseConnection
from utils import (
    recreate_stockcount_monthly_view,
    recreate_stockcount_purchases_view,
    recreate_stockcount_sales_view,
    recreate_stockcount_waste_view,
    recreate_stockcount_conversion_view,
)


def get_conversion_units():
    PurchaseItems = pd.read_csv(
        "./downloads/PurchaseItems.csv",
        usecols=[
            "ID",
            "Name",
            "Weight Qty",
            "Weight UofM",
            "Volume Qty",
            "Volume UofM",
            "Each Qty",
            "Each UofM",
            "Measure Type",
        ],
    )
    PurchaseItems.rename(
        columns={
            "ID": "itemid",
            "Name": "name",
            "Weight Qty": "weight_qty",
            "Weight UofM": "weight_uofm",
            "Volume Qty": "volume_qty",
            "Volume UofM": "volume_uofm",
            "Each Qty": "each_qty",
            "Each UofM": "each_uofm",
            "Measure Type": "measure_type",
        },
        inplace=True,
    )
    RecipeItems = pd.read_csv(
        "./downloads/RecipeItems.csv",
        usecols=[
            "ID",
            "Name",
            "Weight Qty",
            "Weight UofM",
            "Volume Qty",
            "Volume UofM",
            "Each Qty",
            "Each UofM",
            "Measure Type",
        ],
    )
    RecipeItems.rename(
        columns={
            "ID": "itemid",
            "Name": "name",
            "Weight Qty": "weight_qty",
            "Weight UofM": "weight_uofm",
            "Volume Qty": "volume_qty",
            "Volume UofM": "volume_uofm",
            "Each Qty": "each_qty",
            "Each UofM": "each_uofm",
            "Measure Type": "measure_type",
        },
        inplace=True,
    )
    df = pd.concat([PurchaseItems, RecipeItems], ignore_index=True)

    return df


def add_name_column(df, engine):
    # read data from item table
    item = pd.read_sql("SELECT itemid, name FROM item", engine)
    df = df.merge(item, on="itemid", how="left")
    df["name"] = df["name_x"].fillna(df["name_y"])
    df = df.drop(columns=["name_x", "name_y"])
    # drop all rows where name is null
    df = df.dropna(subset=["name"])
    return df


def write_to_database(df, cur, conn, engine):
    table_name = "item_conversion"
    temp_table_name = "temp_table"
    try:
        print(df.info())
        df.to_sql(temp_table_name, engine, if_exists="replace", index=False)
        update_query = sql.SQL(
            """
                insert into {table} (itemid, name, weight_qty, weight_uofm, volume_qty, volume_uofm, each_qty, each_uofm, measure_type)
                select t.itemid, t.name, t.weight_qty, t.weight_uofm, t.volume_qty, t.volume_uofm, t.each_qty, t.each_uofm, t.measure_type
                from {temp_table} AS t
                ON CONFLICT (itemid) DO UPDATE SET
                name = EXCLUDED.name,
                weight_qty = EXCLUDED.weight_qty,
                weight_uofm = EXCLUDED.weight_uofm,
                volume_qty = EXCLUDED.volume_qty,
                volume_uofm = EXCLUDED.volume_uofm,
                each_qty = EXCLUDED.each_qty,
                each_uofm = EXCLUDED.each_uofm,
                measure_type = EXCLUDED.measure_type
                        """
        ).format(
            table=sql.Identifier(table_name),
            temp_table=sql.Identifier(temp_table_name),
        )
        cur.execute(update_query)
    except IntegrityError as e:
        print(e)
    finally:
        try:
            cur.execute("DROP TABLE IF EXISTS temp_table")
            conn.commit()
        except Exception as e:
            print("Error dropping temp table:", e)
            conn.rollback()


if __name__ == "__main__":
    with DatabaseConnection() as db:
        df = get_conversion_units()
        df = add_name_column(df, db.engine)
        write_to_database(df, db.cur, db.conn, db.engine)
        recreate_stockcount_sales_view(db.conn)
        recreate_stockcount_waste_view(db.conn)
        recreate_stockcount_purchases_view(db.conn)
        recreate_stockcount_monthly_view(db.conn)
        recreate_stockcount_conversion_view(db.conn)
