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
