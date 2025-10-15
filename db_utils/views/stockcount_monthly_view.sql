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
