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

from db_utils.dbconnect import DatabaseConnection
from db_utils.recreate_views import recreate_all_views


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
    PurchaseItems = PurchaseItems.rename(
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
    RecipeItems = RecipeItems.rename(
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
    )
    df = pd.concat([PurchaseItems, RecipeItems], ignore_index=True)

    return df


def add_name_column(df, db):
    # read data from item table
    item = pd.read_sql("SELECT itemid, name FROM item", db.engine)
    df = df.merge(item, on="itemid", how="left")
    df["name"] = df["name_x"].fillna(df["name_y"])
    df = df.drop(columns=["name_x", "name_y"])
    # drop all rows where name is null
    df = df.dropna(subset=["name"])
    return df


def write_to_database(df, db):
    table_name = "item_conversion"
    temp_table_name = "temp_table"
    try:
        # Write DataFrame to a temporary table
        df.to_sql(temp_table_name, db.engine, if_exists="replace", index=False)
        # Upsert from temp table into main table
        upsert_query = """
            INSERT INTO item_conversion (itemid, name, weight_qty, weight_uofm, volume_qty, volume_uofm, each_qty, each_uofm, measure_type)
            SELECT itemid, name, weight_qty, weight_uofm, volume_qty, volume_uofm, each_qty, each_uofm, measure_type
            FROM temp_table
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
        db.cur.execute(upsert_query)
        # Delete records not present in the new data
        delete_query = """
            DELETE FROM item_conversion
            WHERE itemid NOT IN (SELECT itemid FROM temp_table)
        """
        db.cur.execute(delete_query)
        db.conn.commit()
    except IntegrityError as e:
        print(e)
        db.conn.rollback()
    except Exception as e:
        print("Error writing to database:", e)
        db.conn.rollback()
    finally:
        try:
            db.cur.execute("DROP TABLE IF EXISTS temp_table")
            db.conn.commit()
        except Exception as e:
            print("Error dropping temp table:", e)
            db.conn.rollback()


if __name__ == "__main__":
    with DatabaseConnection() as db:
        df = get_conversion_units()
        df = add_name_column(df, db)
        write_to_database(df, db)
        # recreate_all_views(db.conn)
