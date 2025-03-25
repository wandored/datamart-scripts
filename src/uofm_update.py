# read UnitOfMeasure.csv file and upload to database

import pandas as pd

# import icecream as ic
from psycopg2.errors import IntegrityError

from dbconnect import DatabaseConnection


# finds and returns the renamed items
def findRenamed(old, new):
    # merge both files on the uofm_id column
    merged = pd.merge(old, new, on="uofm_id")

    # rename for readability
    merged.rename(columns={"name_x": "old_name", "name_y": "new_name"}, inplace=True)

    # check for differences in the name column
    diff = merged[merged["old_name"] != merged["new_name"]]
    return diff


def recreate_stockcount_sales_view(conn):
    create_view_query = """
    CREATE OR REPLACE VIEW stockcount_sales AS
    WITH ingredient_mapping AS (
        -- Create a mapping of 'PREP' ingredients to their actual ingredients
        SELECT 
            ri1.menu_item,
            ri1.ingredient AS prep_ingredient,
            ri2.ingredient AS actual_ingredient
        FROM 
            recipe_ingredients ri1
        JOIN 
            recipe_ingredients ri2 ON ri1.recipe = ri2.menu_item
        WHERE 
            ri1.ingredient LIKE 'PREP%'
    )
    SELECT 
        mi.date,
        mi.dow,
        mi.week,
        mi.period,
        mi.year,
        mi.store_id,
        mi.store,
        mi.menuitem,
        mi.sales_count,
        COALESCE(im.actual_ingredient, ri.ingredient) AS ingredient,
        ri.concept,
        ri.qty * mi.sales_count AS base_usage,
        ri.uofm AS base_uofm,
        CASE
            WHEN ic.weight_qty > 0 THEN 
                CASE
                    WHEN ri.uofm = 'OZ-wt' THEN ri.qty * mi.sales_count / ic.weight_qty
                    WHEN ri.uofm = 'OZ-fl' THEN ri.qty * mi.sales_count / ic.weight_qty
                    WHEN ri.uofm = 'Each' THEN ri.qty * mi.sales_count / ic.weight_qty
                    ELSE 1
                END
            ELSE 1
        END AS count_usage
    FROM 
        menu_item_sales mi
    JOIN recipe_ingredients ri ON ri.menu_item = mi.menuitem
    LEFT JOIN ingredient_mapping im ON im.prep_ingredient = ri.ingredient
    JOIN inv_items ii ON ii.item_name = COALESCE(im.actual_ingredient, ri.ingredient)
    JOIN item_conversion ic ON ic.name = COALESCE(im.actual_ingredient, ri.ingredient)
    WHERE 
        mi.date >= (CURRENT_DATE - '1 mon'::interval) 
        AND ri.concept = mi.concept
    GROUP BY 
        mi.date, mi.dow, mi.week, mi.period, mi.year, mi.store_id, mi.store, 
        mi.menuitem, mi.sales_count, COALESCE(im.actual_ingredient, ri.ingredient), 
        ri.concept, ri.qty, ri.uofm, ic.weight_qty
    ORDER BY 
        mi.date DESC, mi.store, COALESCE(im.actual_ingredient, ri.ingredient);
    """

    with conn.cursor() as cursor:
        cursor.execute(create_view_query)
        conn.commit()

    return


def recreate_stockcount_waste_view(conn):
    create_view_query = """
    CREATE OR REPLACE VIEW stockcount_waste AS
    SELECT sc.date,
        sc.dow,
        sc.week,
        sc.period,
        sc.year,
        sc.store,
        sc.item,
        sc.uofm,
        sum(sc.quantity) AS quantity,
        um.base_uofm,
        sum(sc.quantity) * um.base_qty AS base_qty
    FROM stock_count sc
         JOIN unitsofmeasure um ON um.name = sc.uofm
         JOIN inv_items ii ON ii.item_name::text = sc.item
    WHERE (sc.item IN ( SELECT DISTINCT inv_items.item_name
               FROM inv_items)) AND sc.type = 'Waste Log'::text
    GROUP BY sc.date, sc.dow, sc.week, sc.period, sc.year, sc.store, sc.item, sc.uofm, um.base_qty, um.base_uofm
    ORDER BY sc.date DESC;
    """

    with conn.cursor() as cursor:
        cursor.execute(create_view_query)
        conn.commit()

    return


def recreate_stockcount_purchases_view(conn):
    create_view_query = """
    CREATE OR REPLACE VIEW stockcount_purchases AS
    SELECT sc.date,
        sc.dow,
        sc.week,
        sc.period,
        sc.year,
        sc.store,
        sc.item,
        sc.uofm,
        sum(sc.quantity) AS quantity,
        um.base_uofm,
        sum(sc.quantity) * um.base_qty AS base_qty
    FROM stock_count sc
         JOIN unitsofmeasure um ON um.name = sc.uofm
         JOIN inv_items ii ON ii.item_name::text = sc.item
    WHERE (sc.item IN ( SELECT DISTINCT inv_items.item_name
               FROM inv_items)) AND sc.type = 'Purchase Log'::text
    GROUP BY sc.date, sc.dow, sc.week, sc.period, sc.year, sc.store, sc.item, sc.uofm, um.base_qty, um.base_uofm
    ORDER BY sc.date DESC;
    """
    with conn.cursor() as cursor:
        cursor.execute(create_view_query)
        conn.commit()
    return


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
