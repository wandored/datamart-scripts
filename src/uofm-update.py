# read UnitOfMeasure.csv file and upload to database

import pandas as pd

# import icecream as ic
from psycopg2.errors import IntegrityError

from dbconnect import DatabaseConnection
from utils import (
    recreate_stockcount_sales_view,
    recreate_stockcount_purchases_view,
    recreate_stockcount_waste_view,
    recreate_stockcount_monthly_view,
)


# finds and returns the renamed items
def findRenamed(old, new):
    # merge both files on the uofm_id column
    merged = pd.merge(old, new, on="uofm_id")

    # rename for readability
    merged.rename(columns={"name_x": "old_name", "name_y": "new_name"}, inplace=True)

    # check for differences in the name column
    diff = merged[merged["old_name"] != merged["new_name"]]
    return diff


def main(cur, conn, engine):
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
    uofm.rename(
        columns={
            "ID": "uofm_id",
            "Name": "name",
            "Equivalent Qty": "equivalent_qty",
            "Equivalent UofM": "equivalent_uofm",
            "Measure Type": "measure_type",
            "Base UofM": "base_uofm",
            "Base Qty": "base_qty",
        },
        inplace=True,
    )

    # get the current unitsofmeasure table
    cur.execute("SELECT uofm_id, name FROM unitsofmeasure")
    # store it as a pandas dataframe
    uofm_db = pd.DataFrame(cur.fetchall(), columns=["uofm_id", "name"])
    # find the renamed items
    renamed_items = findRenamed(uofm_db, uofm)

    # ic(renamed_items)

    # psql query to check for the old name in the table transacction_detail and replaces it with the new name
    for index, row in renamed_items.iterrows():
        cur.execute(
            f"UPDATE transaction_detail SET unitofmeasurename = '{row['new_name']}' WHERE unitofmeasurename = '{row['old_name']}'"
        )
        conn.commit()

    # drop table if exists
    cur.execute('drop table if exists "unitsofmeasure" CASCADE')
    conn.commit()
    # update database with table
    try:
        uofm.to_sql("unitsofmeasure", engine, index=False)
        conn.commit()
    except IntegrityError:
        print("Error writing to database: IntegrityError")
        return 1
    except Exception as e:
        print("Error writing to database:", e)
        return 1


if __name__ == "__main__":
    with DatabaseConnection() as db:
        main(db.cur, db.conn, db.engine)
        recreate_stockcount_sales_view(db.conn)
        recreate_stockcount_waste_view(db.conn)
        recreate_stockcount_purchases_view(db.conn)
        recreate_stockcount_monthly_view(db.conn)
