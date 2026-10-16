SELECT
	cc.year,
	cc.period,
	cc.week,
	cr.name AS restaurant,
	td.name AS dining_option,
	ts.display_name,
	sum(ts.quantity) as orders,
	sum(ts.receipt_line_price) as price,
	(sum(ts.receipt_line_price) * sum(ts.quantity)) as total
FROM toast.selections ts
JOIN core.calendar cc
	ON cc.date = DATE(ts.created_date)
JOIN core.restaurants cr
	ON cr.toast_guid = ts.restaurant_id
JOIN toast.dining_options td
	ON td.id = ts.dining_option_id
    AND td.restaurant_id = ts.restaurant_id
WHERE ts.display_name LIKE '%Thanksgiving%'
	AND cc.year = 2026
	AND cc.period >= 10
	AND cc.week >= 1
GROUP BY cc.year, cc.period, cc.week, cr.name, td.name, ts.display_name, ts.created_date
ORDER BY cc.period, cc.week
