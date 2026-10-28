"""Dispatch R365 table syncs; daily resources run by default.

Use --sync reference for infrequently changing tables, or select resources by name.
See docs/r365-api-update.md for schedules and date-window behavior.
"""

import argparse
import logging

from db_utils.r365_utils import R365Client
from src.r365_sync import (
    daily_sales, dates, employees, gl_accounts, inventory_counts, invoices,
    item_categories, jobs, locations, purchase_items, transactions,
    units_of_measure, users, vendor_invoices, vendor_items, vendors,
)

logger = logging.getLogger(__name__)

# Dependency order: reference tables precede resources that refer to them.
# Related child tables are committed by their parent resource's sync.
SYNC_MODULES = {
    "locations": locations,
    "units_of_measure": units_of_measure,
    "gl_accounts": gl_accounts,
    "item_categories": item_categories,
    "vendors": vendors,
    "jobs": jobs,
    "purchase_items": purchase_items,
    "vendor_items": vendor_items,
    "inventory_counts": inventory_counts,
    "transactions": transactions,
    "invoices": invoices,
    "vendor_invoices": vendor_invoices,
    "employees": employees,
    "users": users,
    "daily-sales": daily_sales,
}
REFERENCE_RESOURCES = (
    "units_of_measure", "item_categories", "gl_accounts", "vendors", "jobs",
)
DAILY_RESOURCES = tuple(name for name in SYNC_MODULES if name not in REFERENCE_RESOURCES)
SYNC_GROUPS = {
    "daily": DAILY_RESOURCES,
    "reference": REFERENCE_RESOURCES,
    "all": tuple(SYNC_MODULES),
}


def selected_resources(names):
    selected = set()
    for name in names:
        selected.update(SYNC_GROUPS.get(name, (name,)))
    return [name for name in SYNC_MODULES if name in selected]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--full-vendor-items", action="store_true",
        help="Download and upsert all vendor items only, without date filters.",
    )
    mode.add_argument(
        "--sync", nargs="+", choices=[*SYNC_GROUPS, *SYNC_MODULES], default=None,
        help="Resources or groups to sync in dependency order (default: daily).",
    )
    dates.add_arguments(parser)
    args = parser.parse_args(argv)
    resources = (
        ["vendor_items"] if args.full_vendor_items
        else selected_resources(args.sync or ["daily"])
    )
    if (args.business_date_start or args.business_date_end) and "daily-sales" not in resources:
        parser.error("Business-date filters require the daily-sales sync")
    # Jobs require modification filters. A periodic reference run must cover the
    # interval since the preceding refresh, rather than silently fetching today.
    if (
        args.sync and any(group in args.sync for group in ("reference", "all"))
        and not (args.modified_on_start and args.modified_on_end)
    ):
        parser.error(
            "Reference/all sync requires --modified-on-start and --modified-on-end "
            "covering the last reference refresh"
        )
    try:
        options = {
            name: module.get_options(args) if hasattr(module, "get_options") else {}
            for name, module in SYNC_MODULES.items() if name in resources
        }
        # Preserve date validation even for resources that do not use these flags.
        dates.labor_date_range(args.modified_on_start, args.modified_on_end)
        dates.daily_sales_date_range(args.business_date_start, args.business_date_end)
    except ValueError as exc:
        parser.error(str(exc))
    if args.full_vendor_items:
        options["vendor_items"] = {"full_download": True}
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        client = R365Client()
    except Exception as exc:
        logger.error("R365 sync initialization failed (%s)", type(exc).__name__)
        return 1
    failed = False
    for resource in resources:
        logger.info("Starting R365 %s sync", resource)
        try:
            SYNC_MODULES[resource].sync(client, **options[resource])
        except Exception as exc:
            # API/database exceptions may contain credentials or private records.
            logger.error("R365 %s sync failed (%s)", resource, type(exc).__name__)
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
