# Toast configuration sync

`--sync configuration` retrieves all 24 Configuration v2 endpoints and upserts
one table per endpoint in the `toast` schema. It uses the existing Toast client,
all non-null `core.restaurants.toast_guid` locations, and the shared database writer.
The existing default remains restaurants only.

## Setup and the service-charges migration

Use the updated Orders writer together with this migration. Stop scheduled Orders
imports while switching code and schema: the old writer expects the old table.
Run from the repository root with an already configured `psql` connection:

```sh
psql -X -v ON_ERROR_STOP=1 --single-transaction \
  -f db_utils/schema/toast_configuration_migration.sql \
  -f db_utils/schema/toast_configuration.sql
```

This explicitly removes the old Orders-derived `toast.service_charges` table and
its application amounts, flags, payment links, and refund details. The table name
now belongs to configured service charges. Before removal, the migration adds
`toast.applied_taxes.configured_service_charge_id` and backfills it from the old
application rows, scoped by restaurant. `applied_taxes.service_charge_id` retains
its original meaning: the application GUID from the Orders response. New Orders
syncs keep both that identifier and the configured charge ID, but no longer store
service-charge application rows. They still store selection and service-charge
taxes in `applied_taxes`.

The migration uses `DROP TABLE ... RESTRICT`; dependent database objects cause the
whole transaction to fail. It never drops a configuration-shaped service_charges
table. Reapplying it is safe. New installations can run the same command; install
`toast_orders.sql` separately if using Orders. There are no new foreign keys.
The setup script refuses an old service_charges layout if the migration is omitted.
The importer itself does not execute DDL or drop tables.

For PgAdmin, open `db_utils/schema/toast_configuration_pgadmin.sql` in the Query
Tool and execute the entire script. It combines the migration and table setup
inside an explicit transaction. Use either that combined script or the two-file
psql command above.

## Full load and incremental updates

```sh
# Validate all endpoints and all pages without database writes.
.venv/bin/python -m src.toast-api-update --sync configuration --dry-run

# Initial population: omit the modification filter.
.venv/bin/python -m src.toast-api-update --sync configuration

# Subsequent changes since a known timestamp (offset or Z is required).
.venv/bin/python -m src.toast-api-update --sync configuration \
  --config-last-modified '2026-10-14T00:00:00-04:00'

# Select only menu items/modifiers and modifier groups.
.venv/bin/python -m src.toast-api-update --sync configuration \
  --config-resources menu_items menu_option_groups \
  --config-last-modified '2026-10-14T00:00:00Z'
```

The timestamp is converted to UTC with milliseconds and a compact offset
(`2026-10-14T04:00:00.000+0000`) and passed as `lastModified` on every collection
page. Toast rejects Python's default timestamp serialization. The HTTP
client handles encoding, including `+` in offsets. Omission means a full fetch,
not a truncate/reload. Existing keys are updated and new keys inserted.
No automatic checkpoint is stored: use the start of the previous successful run
(or an earlier overlapping timestamp), and retry from the same cutoff after a
partial failure. `last_synced_at` is local ingestion time, not a Toast modification
watermark. The filter has no upper bound and does not reconstruct historical state.

**Exception:** `/tipWithholding` has no `lastModified` filter or pagination. Its
current singleton object is always refreshed, keyed by restaurant. The source
GUID is retained when present, without inventing one if it is absent.
Live responses can also be an empty `{}`. That means no returned configuration;
the importer skips the write and preserves any existing row.

Toast omits removed/archived configuration entities. Neither an incremental nor
a full run deletes previously stored records or interprets absence as deletion.
See the [Configuration overview](https://doc.toasttab.com/openapi/configuration/overview/),
[menuItems parameters](https://doc.toasttab.com/openapi/configuration/operation/menuItemsGet/),
and [tipWithholding reference](https://doc.toasttab.com/openapi/configuration/operation/tipWithholdingGet/).

## Endpoint and table mapping

Each API path below is relative to `/config/v2/`; resource names are also table
names in `toast` and accepted values for `--config-resources`.

| API path | Resource / table |
| --- | --- |
| alternatePaymentTypes | alternate_payment_types |
| breakTypes | break_types |
| cashDrawers | cash_drawers |
| diningOptions | dining_options |
| discounts | discounts |
| menuGroups | menu_groups |
| menuItems | menu_items |
| menuOptionGroups | menu_option_groups |
| menus | menus |
| noSaleReasons | no_sale_reasons |
| payoutReasons | payout_reasons |
| preModifierGroups | pre_modifier_groups |
| preModifiers | pre_modifiers |
| priceGroups | price_groups |
| printers | printers |
| restaurantServices | restaurant_services |
| revenueCenters | revenue_centers |
| salesCategories | sales_categories |
| serviceAreas | service_areas |
| serviceCharges | service_charges |
| tables | tables |
| taxRates | tax_rates |
| tipWithholding | tip_withholding |
| voidReasons | void_reasons |

## Columns and joins

Collections use `(restaurant_id, id)` as their primary/conflict key. `id` is the
source GUID and `restaurant_id` joins to `core.restaurants.toast_guid`. Always
include restaurant_id in joins: the same configuration GUID can appear in more
than one restaurant. Tip withholding uses restaurant_id alone as its primary key.
All tables retain `entity_type`, optional `external_id`, and `last_synced_at`.
Documented endpoint fields are mapped explicitly to snake_case columns; scalar
values use PostgreSQL text, boolean, bigint, numeric, or UUID types. Missing or
null optional fields become SQL NULL; empty arrays remain empty JSON arrays.
New undocumented source fields need an explicit mapping/schema addition.

Nested arrays and objects (including modifier/group references, images, tax
tables, and charge criteria) are retained as JSONB. Direct references such as
menu, parent, printer, revenue center, and service area also expose UUID columns.
Menu items include modifiers; `type` is not an item-versus-modifier flag. Modifier
group `options` identifies its members. Menu items' `option_groups` excludes
inherited groups, so reporting must account for the menu-group hierarchy and
`inherit_option_groups` when calculating effective modifiers.

Example joining sales selections to menu configuration:

```sql
SELECT s.restaurant_id, s.id AS selection_id, m.id AS menu_item_id,
       m.name, m.sku, m.plu, s.quantity, s.price
FROM toast.selections AS s
LEFT JOIN toast.menu_items AS m
  ON m.restaurant_id = s.restaurant_id AND m.id = s.item_id;
```

Example resolving modifier-group membership:

```sql
SELECT g.restaurant_id, g.id AS modifier_group_id, g.name AS group_name,
       m.id AS modifier_id, m.name AS modifier_name
FROM toast.menu_option_groups AS g
CROSS JOIN LATERAL jsonb_array_elements(g.options) AS member(value)
JOIN toast.menu_items AS m
  ON m.restaurant_id = g.restaurant_id
 AND m.id::text = member.value ->> 'guid';
```

Configuration menuItems does not include a sales-category field. Sales selections
carry `sales_category_id`, which can join to `toast.sales_categories` on restaurant
and GUID. SKU/PLU strings retain leading zeros. Their correspondence to R365 IDs
has not been established; do not assume menu names are globally unique.

## Failure handling and verification

All pages are fetched before a restaurant/endpoint write. Unexpected response
shapes, failed pages, or repeated pagination tokens prevent that batch from being
written. Invalid records or duplicate GUIDs also reject the whole batch. Empty
collections are valid and cause no database writes. Each valid restaurant/endpoint
batch commits atomically; other batches continue after an error. Any API,
validation, or database error makes the command exit with status 1. Counts report
fetched, rejected, skipped, inserted, and updated rows; updated includes unchanged
rows processed through the conflict branch. Dry run reports zero database writes.

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_toast*.py'
```

Optional integration tests use a **fresh isolated** PostgreSQL cluster with socket
`/tmp/toast-config-pg-*`, port 55441, database `postgres`, user `toast_test`, and
`TOAST_CONFIG_TEST_PG_SOCKET` set to its socket directory. They verify legacy
migration, every table's writer, repeat loads, multi-restaurant GUIDs, JSONB/NULLs,
and rollback after a failure on the second write page.
