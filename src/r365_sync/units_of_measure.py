"""R365 units of measure support."""

import pandas as pd

from db_utils.r365_importers import get_units_of_measure
from src.r365_sync.values import convert_uuid
from src.r365_sync.writing import write_to_db


def r365_units_of_measure(client):
    units_of_measure = get_units_of_measure(client)

    df = pd.DataFrame(
        [
            {
                "id": convert_uuid(row["id"]),
                "name": row["name"],
                "is_base": row["isBase"],
                "is_primitive": row["isPrimitive"],
                "is_purchase": row["isPurchase"],
                "is_recipe": row["isRecipe"],
                "is_active": row["isActive"],
                "base_quantity": row["baseQuantity"],
                "equivalent_quantity": row["equivalentQuantity"],
                "measure_type": row["measureType"],
                "equivalent_uom_id": convert_uuid(row["equivalentUnitOfMeasure"]["id"])
                if row["equivalentUnitOfMeasure"]
                else None,
                "base_uom_id": convert_uuid(row["baseUnitOfMeasure"]["id"])
                if row["baseUnitOfMeasure"]
                else None,
            }
            for row in units_of_measure
        ]
    )

    return df


def sync(client):
    frame = r365_units_of_measure(client)
    return write_to_db(frame, "units_of_measure", "r365")
