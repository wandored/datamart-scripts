# Toast / R365 menu-item mapping

`src.menu-item-map` links each restaurant-scoped Toast menu-item UUID to the
concept-level R365 menu-item UUID and numeric POS item ID. It reads the imported
tables, not the external APIs or CSV downloads.

Restaurant identity comes from `toast.menu_items.restaurant_id =
core.restaurants.toast_guid`. The mapping stores `core.restaurants.id` and uses
`core.restaurants.concept` to build the R365 name (`Casual - Burger`, for example).
Restaurants must have both a Toast GUID and R365 GUID configured.

## Preview and write

Run a read-only preview first. The mapping table need not exist for preview:

```sh
.venv/bin/python -m src.menu-item-map
```

Apply `db_utils/schema/core_menu_item_mapping.sql` once, then insert the proposed
unique matches:

```sh
.venv/bin/python -m src.menu-item-map --write
```

The default report is `output/menu_item_mapping_review.csv`; use `--report PATH`
to change it. Each run overwrites that report. The report includes all Toast rows,
statuses, candidate IDs, and existing decisions; it is a preview of matching, not
an audit of committed inserts. The printed inserted count is emitted after commit.
No source tables are modified. New mappings commit in one transaction; database
errors propagate and roll back the insert batch.

## Matching rules

1. Resolve the Toast restaurant to exactly one company restaurant.
2. Prefix the Toast name with that restaurant's concept and ` - `.
3. Normalize case and whitespace only. Require one matching `r365.menu_items.name`.
4. Require one `r365.pos_items.menu_item` matching that R365 menu name.
5. Store the company restaurant ID, Toast UUID, R365 menu UUID, and numeric POS ID.

No fuzzy matching, punctuation removal, global name deduplication, or fallback
across concepts is performed. Missing restaurant links, blank concepts/names,
unmatched names, and multiple candidates remain unresolved. Toast types and R365
types are retained in their source tables; this script does not assume the two
systems use equivalent type labels.

## Existing and manual mappings

The primary key is `(restaurant_id, toast_menu_item_id)`. Repeated imports insert
only new keys. All existing mappings are preserved, including automatic, manual,
and future API mappings. If a new candidate differs from an existing decision,
the report says `existing_conflict`. If source names no longer resolve, the report
uses `existing_unmatched_menu`, `existing_ambiguous_pos`, etc.; the stored mapping
is retained and needs review. Existing mappings whose Toast source row has been
removed are also retained, but do not appear in the source-driven report.

Use the candidate ID columns to investigate unresolved rows. Approved manual
mappings can be inserted or updated in `core.menu_item_mapping` with
`match_method = 'manual'`. The report is not an automatic manual-override input.
A future API identifier link can populate the same table with `match_method =
'api'`; conflicting existing mappings should still be reviewed.

## Reporting joins

Link Toast items to their mappings with both restaurant and item identity:

```sql
SELECT restaurant.name AS restaurant, item.name AS toast_name,
       mapping.r365_menu_item_id, mapping.r365_pos_item_id
FROM toast.menu_items AS item
JOIN core.restaurants AS restaurant ON restaurant.toast_guid = item.restaurant_id
LEFT JOIN core.menu_item_mapping AS mapping
  ON mapping.restaurant_id = restaurant.id
 AND mapping.toast_menu_item_id = item.id;
```

R365 sales use `core.restaurants.r365_guid = r365.sales_details.location_id`
and `mapping.r365_pos_item_id = r365.sales_details.pos_item_id`. Always scope
that join by restaurant. Multiple Toast UUIDs at one restaurant can resolve to
the same R365 POS record; a direct sales-to-mapping join can therefore multiply
sales rows. For filtering, use `EXISTS` against the mapping, or deduplicate mapping
keys at the intended reporting grain before joining. Mapping IDs does not define
how R365 aggregate sales should be allocated between multiple Toast UUIDs.
