from dbconnect import DatabaseConnection


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
        sc.id AS store_id,
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
    GROUP BY sc.date, sc.dow, sc.week, sc.period, sc.year, sc.id, sc.store, sc.item, sc.uofm, um.base_qty, um.base_uofm
    ORDER BY sc.date DESC;
    """

    with conn.cursor() as cursor:
        cursor.execute(create_view_query)
        conn.commit()

    return


def recreate_stockcount_purchases_view(conn):
    create_view_query = """
    CREATE OR REPLACE VIEW stockcount_purchases AS
    SELECT p.date,
        p.dow,
        p.week,
        p.period,
        p.year,
        p.id AS store_id,
        p.store,
        p.item,
        p.quantity,
        p.uofm,
        um.base_qty * p.quantity / ic.weight_qty AS unit_count
    FROM purchases p
        JOIN transaction t ON t.transactionid = p.transactionid
        JOIN unitsofmeasure um ON um.name = p.uofm
        JOIN item_conversion ic ON ic.name = p.item
        JOIN inv_items ii ON ii.item_name::text = ic.name
    WHERE (ic.name IN ( SELECT DISTINCT inv_items.item_name
            FROM inv_items)) AND p.date >= (CURRENT_DATE - '1 year'::interval) AND p.quantity > 0::double precision
    GROUP BY p.transactionid, p.date, p.week, p.period, p.year, p.id, p.store, p.item, p.quantity, p.uofm, um.base_qty, ic.weight_qty
    ORDER BY p.date DESC, p.store, p.item;
    """
    with conn.cursor() as cursor:
        cursor.execute(create_view_query)
        conn.commit()
    return


def recreate_stockcount_monthly_view(conn):
    create_view_query = """
    CREATE OR REPLACE VIEW stockcount_monthly AS
     WITH date_series AS (
         SELECT generate_series(CURRENT_DATE - '29 days'::interval, CURRENT_DATE::timestamp without time zone, '1 day'::interval)::date AS date
        ), all_combinations AS (
         SELECT d.date,
            i.store_id,
            i.id AS item_id,
            i.item_name
           FROM date_series d
             CROSS JOIN inv_items i
        )
    SELECT ac.date,
        ac.item_id,
        ac.item_name,
        ac.store_id,
        COALESCE(ic.count_total, 0) AS count_total
    FROM all_combinations ac
        LEFT JOIN inv_count ic ON ac.item_id = ic.item_id AND ac.store_id = ic.store_id AND ac.date = ic.trans_date
    ORDER BY ac.date DESC, ac.store_id;
    """

    with conn.cursor() as cursor:
        cursor.execute(create_view_query)
        conn.commit()

    return


if __name__ == "__main__":
    with DatabaseConnection() as db:
        recreate_stockcount_purchases_view(db.conn)
        recreate_stockcount_sales_view(db.conn)
        recreate_stockcount_waste_view(db.conn)
        recreate_stockcount_monthly_view(db.conn)
