CREATE OR REPLACE VIEW public.stockcount_conversion
AS
SELECT date(t.date) AS date,
    c.dow,
    c.week,
    c.period,
    c.year,
    r.id AS store_id,
    r.name AS store,
    t.type,
    i.name AS item,
    td.unitofmeasurename AS uofm,
    td.quantity,
    um.base_uofm,
    um.base_qty,
    ic.weight_qty,
        CASE
            WHEN ic.weight_qty IS NULL OR ic.weight_qty = 0::double precision THEN 0::double precision
            ELSE td.quantity * um.base_qty / ic.weight_qty
        END AS each_conversion
FROM transaction_detail td
 JOIN transaction t ON t.transactionid = td.transactionid
 JOIN restaurants r ON r.locationid::text = td.locationid
 JOIN item i ON i.itemid = td.itemid
 JOIN calendar c ON c.date = date(t.date)
 JOIN unitsofmeasure um ON um.name = td.unitofmeasurename
 JOIN item_conversion ic ON ic.name = i.name
WHERE t.type = 'Stock Count'::text AND i.category2 = 'Beef'::text AND date(t.date) >= (CURRENT_DATE - '1 year'::interval)
ORDER BY (date(t.date)) DESC, r.name, i.name;
