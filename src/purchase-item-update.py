"""
purchase_item_update.py

This script updates the purchase_item table in the database by reading data from R365 API to create a comprehensive purchase_item table.
"""

import logging

import pandas as pd
from psycopg2.errors import UniqueViolation

from db_utils.dbconnect import DatabaseConnection
from db_utils.r365_utils import R365Client
from db_utils.r365_importers import get_purchase_items


def main():
    client = R365Client()
    purchase_items_data = get_purchase_items(client)

    PurchaseItems = pd.DataFrame(
        [
            {
                "item_id": row["id"],
                "item_name": row["name"],
                "reporting_uofm": row["reportingUnitOfMeasure"]["name"]
                if row["reportingUnitOfMeasure"]
                else None,
                "inventory_uofm": row["inventoryUnitOfMeasure"]["name"]
                if row["inventoryUnitOfMeasure"]
                else None,
                "category1": row["itemCategory1"]["name"]
                if row["itemCategory1"]
                else None,
                "category2": row["itemCategory2"]["name"]
                if row["itemCategory2"]
                else None,
                "category3": row["itemCategory3"]["name"]
                if row["itemCategory3"]
                else None,
                "cost_account": row["costAccount"]["name"]
                if row["costAccount"]
                else None,
                "inventory_account": row["inventoryAccount"]["name"]
                if row["inventoryAccount"]
                else None,
                "waste_account": row["wasteAccount"]["name"]
                if row["wasteAccount"]
                else None,
                "key_item": row["isKeyItem"],
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
                "active": row["isActive"],
            }
            for row in purchase_items_data
        ]
    )

    records = PurchaseItems[
        [
            "item_id",
            "item_name",
            "reporting_uofm",
            "inventory_uofm",
            "category1",
            "category2",
            "category3",
            "cost_account",
            "inventory_account",
            "waste_account",
            "key_item",
            "weight_qty",
            "weight_uofm",
            "volume_qty",
            "volume_uofm",
            "each_qty",
            "each_uofm",
            "measure_type",
            "active",
        ]
    ].values.tolist()

    with DatabaseConnection() as db:
        try:
            db.executemany(
                """
                INSERT INTO purchase_item (
                    item_id,
                    item_name,
                    reporting_uofm,
                    inventory_uofm,
                    category1,
                    category2,
                    category3,
                    cost_account,
                    inventory_account,
                    waste_account,
                    key_item,
                    weight_qty,
                    weight_uofm,
                    volume_qty,
                    volume_uofm,
                    each_qty,
                    each_uofm,
                    measure_type,
                    active
                )
                VALUES %s
                ON CONFLICT (item_id) DO UPDATE
                SET item_name = EXCLUDED.item_name,
                    reporting_uofm = EXCLUDED.reporting_uofm,
                    inventory_uofm = EXCLUDED.inventory_uofm,
                    category1 = EXCLUDED.category1,
                    category2 = EXCLUDED.category2,
                    category3 = EXCLUDED.category3,
                    cost_account = EXCLUDED.cost_account,
                    inventory_account = EXCLUDED.inventory_account,
                    waste_account = EXCLUDED.waste_account,
                    key_item = EXCLUDED.key_item,
                    weight_qty = EXCLUDED.weight_qty,
                    weight_uofm = EXCLUDED.weight_uofm,
                    volume_qty = EXCLUDED.volume_qty,
                    volume_uofm = EXCLUDED.volume_uofm,
                    each_qty = EXCLUDED.each_qty,
                    each_uofm = EXCLUDED.each_uofm,
                    measure_type = EXCLUDED.measure_type,
                    active = EXCLUDED.active
                """,
                records,
            )
        except UniqueViolation as e:
            logging.error(f"Unique violation error: {e}")
        except Exception as e:
            logging.error(f"Error inserting/updating purchase items: {e}")


if __name__ == "__main__":
    main()
