CREATE OR REPLACE VIEW public.stockcount_sales_toast
AS
WITH ingredient_mapping AS (
    SELECT 
        ri1.menu_item,
        ri1.ingredient AS prep_ingredient,
        ri2.ingredient AS actual_ingredient
    FROM recipe_ingredients ri1
    JOIN recipe_ingredients ri2 
        ON ri1.recipe = ri2.menu_item
    WHERE ri1.ingredient LIKE 'PREP%'
)
SELECT 
    sc.business_date AS date,
    c.dow,
    c.week,
    c.period,
    c.year,
    sc.store_id,
    sc.name AS store,
    sc.menuitem,
    sc.item_count AS sales_count,
    COALESCE(im.actual_ingredient, ri.ingredient) AS ingredient,
    ri.concept,
    ri.qty * sc.item_count::double precision AS base_usage,
    ri.uofm AS base_uofm,
    CASE
        WHEN ic.weight_qty > 0 THEN
            CASE
                WHEN ri.uofm = ANY (ARRAY['OZ-wt','OZ-fl','Each']) 
                THEN ri.qty * sc.item_count::double precision / ic.weight_qty
                ELSE 1.0
            END
        ELSE 1.0
    END AS count_usage
FROM stockcount_detail_toast sc
JOIN recipe_ingredients ri 
    ON ri.menu_item = sc.menuitem
LEFT JOIN ingredient_mapping im 
    ON im.prep_ingredient = ri.ingredient
JOIN inv_items ii 
    ON ii.item_name = COALESCE(im.actual_ingredient, ri.ingredient)
JOIN item_conversion ic 
    ON ic.name = COALESCE(im.actual_ingredient, ri.ingredient)
JOIN calendar c 
    ON c.date = sc.business_date
WHERE sc.business_date >= (CURRENT_DATE - INTERVAL '1 month')
  AND ri.concept = (
      SELECT DISTINCT concept
      FROM recipe_ingredients
      WHERE recipe_ingredients.menu_item = sc.menuitem
      LIMIT 1
  )
GROUP BY 
    sc.business_date, c.dow, c.week, c.period, c.year,
    sc.store_id, sc.name, sc.menuitem, sc.item_count,
    COALESCE(im.actual_ingredient, ri.ingredient),
    ri.concept, ri.qty, ri.uofm, ic.weight_qty
ORDER BY sc.business_date DESC, sc.name, COALESCE(im.actual_ingredient, ri.ingredient);
