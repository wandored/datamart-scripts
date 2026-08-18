"""
This script reads data from PurchaseItems API, processes it by
renaming columns for consistency, and then writes the data into a PostgreSQL
database table named `item_conversion`. It uses `pandas` to handle the CSV file,
`sqlalchemy` to establish a connection to the database, and `psycopg2` for
executing SQL queries. The script performs an upsert operation, updating
existing records in the `item_conversion` table or inserting new ones, and
ensures temporary tables are cleaned up after execution.
Error handling is included to manage integrity errors and cleanup failures.
"""

import pandas as pd
import logging
from psycopg2.errors import UniqueViolation

from db_utils.dbconnect import DatabaseConnection
from db_utils.r365_utils import R365Client
from db_utils.r365_importers import get_purchase_items


def get_conversion_units():
    client = R365Client()
    results = get_purchase_items(client)

    PurchaseItems = pd.DataFrame(
        [
            {
                "itemid": row["id"],
                "name": row["name"],
                "weight_qty": row["equivalenceWeightQuantity"],
                "weight_uofm": row["equivalenceWeightUnitOfMeasure"]["name"]
                if row["equivalenceWeightUnitOfMeasure"]
                else None,
                "volume_qty": row["equivalenceVolumeQuantity"],
                "volume_uofm": row["equivalenceVolumeUnitOfMeasure"]["name"]
                if row["equivalenceVolumeUnitOfMeasure"]
                else None,
                "each_qty": row["equivalenceEachQuantity"],
                "each_uofm": row["equivalenceEachUnitOfMeasure"]["name"]
                if row["equivalenceEachUnitOfMeasure"]
                else None,
                "measure_type": row["measureType"],
            }
            for row in results
            if row["isActive"]
        ]
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


def write_to_database(df):
    records = df[
        [
            "itemid",
            "name",
            "weight_qty",
            "weight_uofm",
            "volume_qty",
            "volume_uofm",
            "each_qty",
            "each_uofm",
            "measure_type",
        ]
    ].values.tolist()
    with DatabaseConnection() as db:
        try:
            db.executemany(
                """
               INSERT INTO item_conversion (itemid, name, weight_qty, weight_uofm, volume_qty, volume_uofm, each_qty, each_uofm, measure_type)
               VALUES %s
               ON CONFLICT (itemid) DO UPDATE
               SET name = EXCLUDED.name,
                   weight_qty = EXCLUDED.weight_qty,
                   weight_uofm = EXCLUDED.weight_uofm,
                   volume_qty = EXCLUDED.volume_qty,
                   volume_uofm = EXCLUDED.volume_uofm,
                   each_qty = EXCLUDED.each_qty,
                   each_uofm = EXCLUDED.each_uofm,
                   measure_type = EXCLUDED.measure_type
                """,
                records,
            )
        except UniqueViolation as e:
            logging.error(f"Unique violation error: {e}")
        except Exception as e:
            logging.error(f"Error inserting/updating item_conversion: {e}")


if __name__ == "__main__":
    df = get_conversion_units()
    df = df.dropna(subset=["name"])
    write_to_database(df)
