CREATE OR REPLACE VIEW stockcount_purchases AS
SELECT p.transactionid,
    p.date,
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
       FROM inv_items)) AND p.date >= (CURRENT_DATE - '1 year'::interval) AND p.quantity > 0
GROUP BY p.transactionid, p.date, p.dow, p.week, p.period, p.year, p.id, p.store, p.item, p.quantity, p.uofm, um.base_qty, ic.weight_qty
ORDER BY p.date DESC, p.store, p.item;
