# read UnitOfMeasure.csv file and upload to database
import pandas as pd
from db_utils.dbconnect import DatabaseConnection


def find_renamed(old, new):
    merged = pd.merge(old, new, on="uofm_id")
    merged.rename(columns={"name_x": "old_name", "name_y": "new_name"}, inplace=True)
    diff = merged[merged["old_name"] != merged["new_name"]]
    return diff


def main():
    file_path = "./downloads/UnitOfMeasure.csv"
    uofm = pd.read_csv(
        file_path,
        usecols=[
            "ID",
            "Name",
            "Equivalent Qty",
            "Equivalent UofM",
            "Measure Type",
            "Base UofM",
            "Base Qty",
        ],
    )
    uofm = uofm.rename(
        columns={
            "ID": "uofm_id",
            "Name": "name",
            "Equivalent Qty": "equivalent_qty",
            "Equivalent UofM": "equivalent_uofm",
            "Measure Type": "measure_type",
            "Base UofM": "base_uofm",
            "Base Qty": "base_qty",
        }
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
            ]
        ].values.tolist()

        db.executemany(
            """
            INSERT INTO unitsofmeasure (uofm_id, name, equivalent_qty, equivalent_uofm, measure_type, base_uofm, base_qty)
            VALUES %s
            ON CONFLICT (uofm_id) DO UPDATE
            SET name = EXCLUDED.name,
                equivalent_qty = EXCLUDED.equivalent_qty,
                equivalent_uofm = EXCLUDED.equivalent_uofm,
                measure_type = EXCLUDED.measure_type,
                base_uofm = EXCLUDED.base_uofm,
                base_qty = EXCLUDED.base_qty
            """,
            records,
        )
        print("database uploaded")


if __name__ == "__main__":
    main()
