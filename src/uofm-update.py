# read UnitOfMeasure.csv file and upload to database
import pandas as pd
from db_utils.dbconnect import DatabaseConnection
from db_utils.r365_utils import R365Client
from db_utils.r365_importers import get_units_of_measure


def find_renamed(old, new):
    merged = pd.merge(old, new, on="uofm_id")
    merged.rename(columns={"name_x": "old_name", "name_y": "new_name"}, inplace=True)
    diff = merged[merged["old_name"] != merged["new_name"]]
    return diff


def main():
    client = R365Client()
    uofm_data = get_units_of_measure(client)

    uofm = pd.DataFrame(
        [
            {
                "uofm_id": row["id"],
                "name": row["name"],
                "equivalent_qty": row["equivalentQuantity"],
                "equivalent_uofm": row["equivalentUnitOfMeasure"]["name"]
                if row["equivalentUnitOfMeasure"]
                else None,
                "measure_type": row["measureType"],
                "base_uofm": row["baseUnitOfMeasure"]["name"]
                if row["baseUnitOfMeasure"]
                else None,
                "base_qty": row["baseQuantity"],
                "active": row["isActive"],
            }
            for row in uofm_data
        ]
    )

    with DatabaseConnection() as db:
        # find renamed items and update transaction_detail
        db.execute("SELECT uofm_id, name FROM unitsofmeasure")
        uofm_db = pd.DataFrame(db.fetchall(), columns=["uofm_id", "name"])
        renamed_items = find_renamed(uofm_db, uofm)

        if not renamed_items.empty:
            rename_records = list(
                zip(renamed_items["new_name"], renamed_items["old_name"])
            )
            db.executemany(
                """
                UPDATE transaction_detail
                SET unitofmeasurename = v.new_name
                FROM (VALUES %s) AS v(new_name, old_name)
                WHERE unitofmeasurename = v.old_name
                """,
                rename_records,
            )

        # upsert unitsofmeasure
        uofm = uofm.astype(str).replace("nan", None)
        uofm = uofm.drop_duplicates(subset=["uofm_id"], keep="last")
        records = uofm[
            [
                "uofm_id",
                "name",
                "equivalent_qty",
                "equivalent_uofm",
                "measure_type",
                "base_uofm",
                "base_qty",
                "active",
            ]
        ].values.tolist()

        db.executemany(
            """
            INSERT INTO unitsofmeasure (uofm_id, name, equivalent_qty, equivalent_uofm, measure_type, base_uofm, base_qty, active)
            VALUES %s
            ON CONFLICT (uofm_id) DO UPDATE
            SET name = EXCLUDED.name,
                equivalent_qty = EXCLUDED.equivalent_qty,
                equivalent_uofm = EXCLUDED.equivalent_uofm,
                measure_type = EXCLUDED.measure_type,
                base_uofm = EXCLUDED.base_uofm,
                base_qty = EXCLUDED.base_qty,
                active = EXCLUDED.active
            """,
            records,
        )
        print("database uploaded")


if __name__ == "__main__":
    main()
