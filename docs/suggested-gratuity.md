# Suggested Gratuity check lookup

```bash
.venv/bin/python -m src.suggested_gratuity --employee-ids-from-db
```

Reads `downloads/suggested_gratuity.csv` and writes
`output/suggested_gratuity_details.csv`. Use `--input`, `--output`, or `--match`
to override the defaults. Omit `--employee-ids-from-db` to run without PostgreSQL.
All external operations are reads; no source tables are updated.

The fastest route for an existing sales-detail export is OData:
`SalesDetail.salesdetailID` identifies each CSV line, `SalesDetail.salesID` joins
to `SalesEmployee.salesId`, and `location` joins to `Location.locationId`.
The public daily-sales API could retrieve these tickets too, but requires paging
through each location/business day's sales. The current local
`r365.sales_details` table aggregates items and no longer retains individual
sales-detail IDs. The older OData importer drops the ticket link and check fields.

The output preserves the CSV columns and adds:

| Column | Meaning |
| --- | --- |
| `location_name` | Location name from OData |
| `receipt_number` | Original R365 POS receipt text; not a verified separate Toast order number |
| `check_number` | POS check number supplied to R365; leading zeros preserved |
| `server_name` | Raw OData `server` value, which is a name in the verified live response |
| `employee_id` | Optional R365 server GUID from an exact ticket-ID lookup in current `r365.sales_tickets`; blank if absent |
| `manager_id` | Blank: neither R365 sales endpoint exposes a manager ID |
| `sales_id` | R365 ticket GUID |
| `match_status` | `matched` or a specific missing/mismatched-record reason |

There is one output row per matching input line, so checks with multiple matching
items appear more than once. Filtering is a case-insensitive substring match;
voided items are retained and the original `void` column remains available.
Missing records remain in the output. Re-polled/deleted DSS records can invalidate
old sales-detail IDs; the script does not guess replacements by date or amount.

Requests use batches of at most 10 GUIDs (below the service's filter-complexity
limit), date windows within the 31-day limit,
and OData continuation links. The date window includes the preceding and following
day to allow for differences between item and check timestamps. Unusually long-open
checks outside that window can be reported as missing. A failed request stops the
run before writing a new report. If a previous report exists, it remains unchanged.

A distinct Toast order number or an approving manager requires a separately
verified POS lookup; receipt text and R365 audit users must not be substituted.

## Toast enrichment

```bash
.venv/bin/python -m src.suggested_gratuity_toast
```

Reads the enriched R365 CSV and writes `output/suggested_gratuity_toast.csv`.
Restaurant mappings come from `core.restaurants.r365_guid/toast_guid`. The script
pages through Toast orders for each restaurant and date from the R365 receipt,
then matches the check number, Suggested Gratuity name substring, and amount.
Duplicate candidates are flagged as ambiguous. Missing or changed charges are
flagged rather than guessing a match. All input rows remain in the output.

The receipt's explicit date is used because a verified source row stores local
wall-clock time with a `+00` suffix. Converting that timestamp as UTC would shift
it incorrectly. Toast's returned business date is checked against the requested
date. Checks that belong to a different business day remain unmatched for review.

Added columns include `toast_order_number`, `toast_check_number`, order/check GUIDs,
and `toast_employee_id` (the order's server GUID, distinct from an R365 employee ID).
In the verified live sample, Suggested Gratuity is an applied service charge.
Toast's Orders API does not expose who approved adding/changing that charge.
Toast also [confirms that the gratuity/service-charge approver cannot currently be located](https://support.toasttab.com/en/article/Can-I-require-manager-approval-to-add-service-charges).
`toast_gratuity_approval_status` states this explicitly; `manager_id` stays unchanged.

`toast_discount_approver_ids` lists approvers of discounts on the matched check or
its selections. `toast_payment_void_approver_ids` lists payment-void approvers.
These IDs do **not** identify who approved Suggested Gratuity, and they do not
independently prove that an employee has a manager role. Multiple IDs are separated
by semicolons.

Compact daily results are cached under `output/suggested_gratuity_toast_cache`
without customer information or payment details. Re-runs reuse those snapshots;
pass `--refresh` to retrieve current Toast data. `--input`, `--output`, and
`--cache-dir` can override the paths. No database writes are performed.

Toast references: [bulk orders](https://doc.toasttab.com/openapi/orders/operation/ordersBulkGet/),
[service charges](https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/AppliedServiceCharge/),
[discount approvers](https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/AppliedDiscount/),
and [void information](https://doc.toasttab.com/openapi/orders/tag/Data-definitions/schema/VoidInformation/).

References: [R365 OData connector](https://docs.restaurant365.com/docs/restaurant365-odata-connector)
and [public daily-sales API](https://docs.restaurant365.com/apidocs/public-v1-sales-daily-sales).
