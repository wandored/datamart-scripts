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
