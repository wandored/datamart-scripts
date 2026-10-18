SELECT DISTINCT
    v_ns.nspname AS schema_name,
    v.relname AS view_name,
    CASE v.relkind
        WHEN 'v' THEN 'VIEW'
        WHEN 'm' THEN 'MATERIALIZED VIEW'
    END AS view_type
FROM pg_class t
JOIN pg_namespace t_ns
    ON t_ns.oid = t.relnamespace
JOIN pg_depend d
    ON d.refobjid = t.oid
JOIN pg_rewrite r
    ON r.oid = d.objid
JOIN pg_class v
    ON v.oid = r.ev_class
JOIN pg_namespace v_ns
    ON v_ns.oid = v.relnamespace
WHERE t_ns.nspname = 'public'
  AND t.relname = 'menu_items'
  AND v.relkind IN ('v', 'm')
  AND d.classid = 'pg_rewrite'::regclass
  AND d.refclassid = 'pg_class'::regclass
  AND d.deptype = 'n'
ORDER BY schema_name, view_name;
