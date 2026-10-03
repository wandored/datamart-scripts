# Toast orders

Apply `db_utils/schema/toast_orders.sql` explicitly to the intended database.
The [complete table definitions](../db_utils/schema/toast_orders.sql) create all
seven Orders tables if absent and add `orders.last_synced_at` to the previous
header-only table. Existing order columns and types are preserved, including
`calculated_guest_count NUMERIC` and the established required `business_date`.
No tables are replaced and no foreign keys are added. The updater performs no DDL.
Old order rows initially receive the migration time as `last_synced_at`; re-fetch
their business dates before using that timestamp to reconcile children.

| Table | Grain and key | Parent context |
| --- | --- | --- |
| orders | Order; `id = guid`, UUID PK | restaurant_id |
| checks | Check; `id = guid`, UUID PK | order_id, restaurant_id |
| selections | Item or modifier; `id = guid`, UUID PK | check_id, order_id, restaurant_id, parent_selection_id |
| payments | Payment; `id = guid`, UUID PK | check_id, order_id, restaurant_id |
| applied_discounts | Discount application; `id = guid`, UUID PK | check_id, order_id, restaurant_id; nullable selection_id |
| service_charges | Service-charge application; `id = guid`, UUID PK | check_id, order_id, restaurant_id |
| applied_taxes | Tax application; `id = guid`, UUID PK | check_id, order_id, restaurant_id; exactly one of selection_id/service_charge_id |

All tables carry the order's `business_date` and a UTC `last_synced_at` observation
time. Child GUID references are nullable unless they identify a required parent.
Order indexes cover `(restaurant_id, business_date)`, `business_date`, and
`modified_date`. The composite index also supports restaurant-only lookups.
Child indexes cover order/check joins, modifier parents, and tax parents.

```sh
psql -X -v ON_ERROR_STOP=1 -f db_utils/schema/toast_orders.sql
.venv/bin/python -m src.toast-api-update --sync orders --business-date 2026-09-30 --dry-run
.venv/bin/python -m src.toast-api-update --sync orders --business-date 2026-09-30
.venv/bin/python -m src.toast-api-update --sync orders --business-date-start 2026-09-01 --business-date-end 2026-09-30
.venv/bin/python -m src.toast-api-update --sync restaurants orders --business-date 2026-09-30
```

Order runs require explicit dates. Ranges include both endpoints; a single range
boundary selects one day. A single date cannot be combined with range arguments.
The default command without `--sync` still updates only restaurants. Dry run
fetches and validates all pages but writes no database rows.

## Retrieval and writes

Scope comes from every non-null `core.restaurants.toast_guid`, as for restaurants.
Each restaurant/day calls `/orders/v2/ordersBulk` with `businessDate=yyyyMMdd`.
The existing Toast authentication, request retries, and page-number helper are
reused. Pages contain at most 100 orders; a full page triggers another request
even if the Link header is absent. All pages must succeed before that day's rows
are written. Empty days succeed and leave stored data unchanged.

`src/toast_sync/orders.py` retrieves and normalizes all seven tables; the shared
`src/toast_sync/writing.py` bulk-upserts by `id` using `DatabaseConnection`.
Each restaurant/day is one transaction across every table, with no deletes or truncation. Repeat
runs update existing GUIDs. Voided, deleted, test-mode, and excess-food orders
are retained with their flags. No totals are calculated from checks or payments.

Malformed nested objects reject the entire parent order, with contextual errors
containing endpoint, restaurant, date, order and check IDs. Other valid orders
can update. Identical complete order graphs are deduplicated; conflicting copies
of an order are skipped. A conflicting child GUID shared by different orders
rejects that restaurant/day batch instead of silently overwriting a parent.
API failures write nothing for that
restaurant/day; database failures roll back that day's transaction. Other days
and restaurants continue. Any API/database error or rejected row gives exit
status 1. The final summary reports inserted/updated counts for each table after
commit, using the existing `RETURNING` counting method, with no per-row count
queries. Counts also include requested restaurant/day batches, fetched orders,
duplicates, rejected rows, committed inserts/updates, and errors. Unchanged rows
count as updated when they take the upsert update branch.

This is an explicit business-date re-fetch, not a modified-date watermark sync.
Re-run affected historical business dates to capture later changes, including
orders moved to another business date. A row absent from an API response is not
deleted or inferred to be voided. No automatic scheduling/lookback is introduced.
`normalize_order`/`flatten_order` also accept no expected business date, so future
modified-window retrieval can reuse this table model. Business-date requests let
Toast apply its restaurant-specific closeoutHour (currently 04:00 for our sites);
the importer never recalculates business dates using midnight or a hard-coded hour.

## Mapping

The [Order schema](https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/Order/)
defines the source fields below. GUID references are flattened without foreign
keys; optional null/missing fields become SQL NULL. `businessDate` comes directly
from Toast and must match the requested day. Offset-bearing timestamps become
TIMESTAMPTZ; no server-local time zone is inferred. The epoch `deletedDate` sentinel
is preserved. Unset `voidBusinessDate` values (null or 0) become SQL NULL.

| Column(s) | Source |
| --- | --- |
| id | guid (UUID primary key) |
| restaurant_id | Requested restaurant GUID (UUID) |
| business_date | businessDate (integer yyyyMMdd → DATE) |
| external_id, display_number | externalId, displayNumber (TEXT; leading zeros retained) |
| source, approval_status | source, approvalStatus |
| created_by_client_name, required_prep_time | createdByClientName, requiredPrepTime |
| number_of_guests, duration | numberOfGuests, duration (INTEGER) |
| calculated_guest_count | calculatedGuestCount (NUMERIC) |
| opened_date, modified_date, promised_date, created_date | openedDate, modifiedDate, promisedDate, createdDate |
| paid_date, closed_date, deleted_date, void_date | paidDate, closedDate, deletedDate, voidDate |
| estimated_fulfillment_date | estimatedFulfillmentDate |
| void_business_date | voidBusinessDate (DATE) |
| voided, deleted, created_in_test_mode, excess_food | voided, deleted, createdInTestMode, excessFood |
| server_id, dining_option_id, table_id | server.guid, diningOption.guid, table.guid |
| service_area_id, restaurant_service_id, revenue_center_id | serviceArea.guid, restaurantService.guid, revenueCenter.guid |
| channel_id | channelGuid |
| created_device_id, last_modified_device_id | createdDevice.id, lastModifiedDevice.id (TEXT device identifiers) |
| pricing_features | pricingFeatures (TEXT[]) |
| last_synced_at | UTC observation timestamp assigned to the entire restaurant/day batch |

## API discrepancies and child normalization

`calculatedGuestCount` is fractional, not an integer; it remains NUMERIC.
Monetary values, quantities, rates, percentages, and weights use NUMERIC. The
Orders fetch decodes JSON fractions directly as Decimal. Missing values stay
NULL; integer semantics such as seat number, duration, and number of guests stay
INTEGER. Timestamp inputs must carry an offset; epoch deletion timestamps are
preserved. Optional business-date values of 0 are normalized to NULL.

The [Check model](https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/Check/)
supports the proposed check fields; `opened_by_id` maps to `openedBy.guid`.
Its taxAmount is an aggregate and is stored only as `checks.tax_amount`.
There is no check-level tax-entry array to import.

The [Selection model](https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/Selection/)
spells the source PLU `premodifierPlu`. Both `openPriceAmount` and
`externalPriceAmount` are POST-only and normally absent in GET responses. Their
requested columns remain NULL when absent; receiptLinePrice is preserved rather
than reverse-calculated. Additional documented void/refund references and amounts
are retained. Names and concept prefixes are unchanged, and voided items remain.

`walk_selections` adapts product-mix's visit-item/visit-modifiers traversal with
an explicit depth-first stack. A top-level selection gets parent_selection_id
NULL. Each modifier gets its immediate parent's GUID, including deeply nested
modifiers. No item is discarded because its price is zero or its item reference
is absent. Each selection's discounts and taxes are visited in the same traversal.

The [Payment model](https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/Payment/)
supplies refund details under `refund` and void details under `voidInfo`.
Payments retain their own paid/refund business dates alongside the inherited
order business date. Split payments stay separate by GUID. Card fragments,
processor/card payment identifiers, customer objects, and raw payload JSON are
not stored. Card type and entry mode are retained as requested.

[AppliedDiscount](https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/AppliedDiscount/)
is the same model at check and selection levels. A NULL selection_id indicates
check scope. Each application has its own required GUID; discount_id is the
separate configuration reference. Amount, percentage, non-tax amount, processing
state, promo code, approver, and applied reason are retained. Combo/trigger arrays
and loyalty-account details are not represented in this first normalized set.

[AppliedServiceCharge](https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/AppliedServiceCharge/)
has its own GUID. `service_charges.service_charge_id` references the configured
charge, while `applied_taxes.service_charge_id` references the application row.
The applied object provides no percentage/rate field, so no rate is inferred.
Amount, type, calculation/category, gratuity/taxable/dining flags, payment link,
and refund details are retained.

[AppliedTaxRate](https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/AppliedTaxRate/)
is shared by selections and service charges. Its required application GUID is
the PK; tax_rate_id preserves the separate configured tax reference. A local
CHECK constraint ensures exactly one of selection_id and service_charge_id is
set. Selection tax inclusion is copied from the containing selection; service
charge taxes have NULL inclusion because that parent exposes no inclusion field.
No order-level marketplace facilitator taxes are fabricated: that object is
POST-only. Required application GUIDs are never synthesized; missing or
conflicting IDs cause contextual validation errors.

The constant entityType, delivery/curbside/provider details and packaging are
not stored. All retained child field paths/types are explicit in `CHILD_FIELDS`
in the Orders module; the SQL file contains the final physical table definitions.

## Children absent on a later retrieval

UPSERT does not remove a previously observed child when it disappears from the
API. Those rows may be stale. This implementation intentionally performs no
deletes, no void inference, and no automatic inactivation. All rows successfully
seen in one order graph receive the same last_synced_at. For example, investigate
selection rows older than their parent order:

```sql
SELECT s.id, s.order_id, s.check_id, s.last_synced_at, o.last_synced_at AS order_synced_at
FROM toast.selections AS s
JOIN toast.orders AS o ON o.id = s.order_id
WHERE s.last_synced_at < o.last_synced_at;
```

This identifies candidates for reconciliation, not proof of deletion. Before
automating reconciliation, verify real examples of check moves, removed modifiers,
discount removals, refunds, and voids. Recommended next step: a non-destructive
`is_current`/`last_seen_at` policy scoped to fully retrieved orders, after confirming
these semantics. Until then, reports should explicitly decide how to handle
potentially stale children rather than summing the historical tables blindly.

## Verification

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_toast*sync.py'
```

Optional PostgreSQL tests use the same fresh isolated `/tmp/toast-restaurants-pg-*`
socket setup described in `toast-api-update.md`, enabled by `TOAST_TEST_PG_SOCKET`.
They exercise additive upgrades, bulk counts, mixed inserts/updates, rollback
across all seven tables, timestamp refresh, and retention of missing children.
Unit tests cover multiple checks, split payments, voids, discounts, taxes,
service charges, optional nulls, and 1,100 levels of modifiers. Tests make no
live Toast calls.

Before proceeding to Labor, compare a single day's counts and sample GUIDs with
Toast, inspect an order opened after midnight for its correct business date,
check source flags/reference IDs, and run the same date twice. Confirm that the
second run updates existing rows without increasing row count and that
`orders.restaurant_id` joins logically to `restaurants.id` and
`core.restaurants.toast_guid`. Verify application GUID presence, uniqueness across
parents, and stability across repeat API calls; missing-ID cases must be reviewed
before introducing any deterministic fallback. Check monetary precision and
refunds, confirm tax totals without double-counting selection/check aggregates,
review the stale-child candidates, and verify all configured restaurants have access.
