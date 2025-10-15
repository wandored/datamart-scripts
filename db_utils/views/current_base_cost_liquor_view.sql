CREATE OR REPLACE VIEW public.current_base_cost_liquor
 AS
 WITH ranked AS (
         SELECT r.id AS store_id,
            r.concept,
            r.name AS store,
            i.name AS item,
            td.unitofmeasurename AS uofm,
            um.base_uofm,
            um.base_qty,
            td.amount / NULLIF(td.quantity, 0::double precision) AS amount,
            td.amount / NULLIF(td.quantity, 0::double precision) / NULLIF(um.base_qty, 0::double precision) AS base_amount,
            t.date,
            row_number() OVER (PARTITION BY r.id, i.name ORDER BY t.date) AS rn
           FROM transaction_detail td
             JOIN transaction t ON t.transactionid = td.transactionid
             JOIN glaccount gl ON gl.glaccountid = td.glaccountid
             JOIN restaurants r ON r.locationid::text = td.locationid
             JOIN item i ON i.itemid = td.itemid
             JOIN unitsofmeasure um ON um.name = td.unitofmeasurename
          WHERE gl.name = 'Liquor'::text AND t.type = 'AP Invoice'::text
        )
 SELECT ranked.store_id,
    ranked.concept,
    ranked.store,
    ranked.item,
    ranked.uofm,
    round(ranked.amount::numeric, 2) AS amount,
    ranked.base_uofm,
    round(ranked.base_amount::numeric, 4) AS base_amount
   FROM ranked
  WHERE ranked.rn = 1
  ORDER BY ranked.item, ranked.store;

