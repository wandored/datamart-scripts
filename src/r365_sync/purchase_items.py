"""R365 purchase items support."""

import pandas as pd

from db_utils.r365_importers import get_purchase_items
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def r365_purchase_items(client):
    purchase_items = get_purchase_items(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "name": row["name"],
                "number": row["number"],
                "is_active": row["isActive"],
                "cost_account_id": convert_uuid(row["costAccount"]["id"])
                if row["costAccount"]
                else None,
                "inventory_account_id": convert_uuid(row["inventoryAccount"]["id"])
                if row["inventoryAccount"]
                else None,
                "waste_account_id": convert_uuid(row["wasteAccount"]["id"])
                if row["wasteAccount"]
                else None,
                "donation_account_id": convert_uuid(row["donationAccount"]["id"])
                if row["donationAccount"]
                else None,
                "cost_update_method": row["costUpdateMethod"],
                "description": row["description"],
                "reporting_uom_id": convert_uuid(row["reportingUnitOfMeasure"]["id"])
                if row["reportingUnitOfMeasure"]
                else None,
                "inventory_uom_id": convert_uuid(row["inventoryUnitOfMeasure"]["id"])
                if row["inventoryUnitOfMeasure"]
                else None,
                "inventory_uom_2_id": convert_uuid(row["inventoryUnitOfMeasure2"]["id"])
                if row["inventoryUnitOfMeasure2"]
                else None,
                "inventory_uom_3_id": convert_uuid(row["inventoryUnitOfMeasure3"]["id"])
                if row["inventoryUnitOfMeasure3"]
                else None,
                "equivalence_each_uom_id": convert_uuid(
                    row["equivalenceEachUnitOfMeasure"]["id"]
                )
                if row["equivalenceEachUnitOfMeasure"]
                else None,
                "equivalence_each_quantity": row["equivalenceEachQuantity"],
                "equivalence_volume_uom_id": convert_uuid(
                    row["equivalenceVolumeUnitOfMeasure"]["id"]
                )
                if row["equivalenceVolumeUnitOfMeasure"]
                else None,
                "equivalence_volume_quantity": row["equivalenceVolumeQuantity"],
                "equivalence_weight_uom_id": convert_uuid(
                    row["equivalenceWeightUnitOfMeasure"]["id"]
                )
                if row["equivalenceWeightUnitOfMeasure"]
                else None,
                "equivalence_weight_quantity": row["equivalenceWeightQuantity"],
                "item_category_1_id": convert_uuid(row["itemCategory1"]["id"])
                if row["itemCategory1"]
                else None,
                "item_category_2_id": convert_uuid(row["itemCategory2"]["id"])
                if row["itemCategory2"]
                else None,
                "item_category_3_id": convert_uuid(row["itemCategory3"]["id"])
                if row["itemCategory3"]
                else None,
                "is_key_item": row["isKeyItem"],
                "type": row["type"],
                "measure_type": row["measureType"],
            }
            for row in purchase_items
        ]
    )

    return df


def sync(client):
    frame = r365_purchase_items(client)
    return write_to_db(frame, "purchase_items", "r365")
