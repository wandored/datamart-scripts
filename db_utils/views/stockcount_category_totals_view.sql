CREATE OR REPLACE VIEW stockcount_category_totals
AS
SELECT sc.date,
    sc.week,
    sc.period,
    sc.year,
    sc.id AS store_id,
    sc.store,
    sc.account,
    sc.category1,
    sum(sc.previous_amount) AS previous,
    sum(sc.amount) AS amount
FROM stock_count sc
WHERE sc.type = 'Stock Count'::text
GROUP BY sc.date, sc.week, sc.period, sc.year, sc.id, sc.store, sc.account, sc.category1
ORDER BY sc.date DESC, sc.id;
