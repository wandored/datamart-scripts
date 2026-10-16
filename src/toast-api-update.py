"""Dispatch Toast source syncs for restaurants, orders, labor, and configuration.

Apply the SQL for each selected resource first. See docs/toast-api-update.md.
"""

import argparse
import logging
from datetime import date

from db_utils.toast_utils import ToastClient
from db_utils import fiscal_calendar
from src.toast_sync.locations import get_configured_restaurants
from src.toast_sync import configuration, dates, labor, orders, restaurants

logger = logging.getLogger(__name__)

# Each module exposes sync(client, locations, dry_run=False, **resource_options).
# Optional add_arguments/get_options hooks keep resource-specific CLI logic local.
SYNC_MODULES = {
    "restaurants": restaurants,
    "orders": orders,
    "labor": labor,
    "configuration": configuration,
}


def positive_integer(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def sync_options(args):
    dates.business_date_options(args)
    return {
        name: module.get_options(args) if hasattr(module, "get_options") else {}
        for name, module in SYNC_MODULES.items()
    }


def prepare_runs(args):
    """Validate selections before calendar reads, then resolve dated run options."""
    scheduled = args.daily or args.weekly or args.period is not None
    if not scheduled:
        if args.year is not None or args.week is not None:
            raise ValueError("--year and --week require --period")
        args.sync = args.sync or ["restaurants"]
        if "all" in args.sync:
            args.sync = list(SYNC_MODULES)
        return [sync_options(args)]

    if any((args.business_date, args.business_date_start, args.business_date_end,
            args.labor_modified_start, args.labor_modified_end)):
        raise ValueError("Sync modes cannot be combined with explicit business dates or Labor modification timestamps")
    if args.period is not None:
        if args.year is None:
            raise ValueError("--period requires --year")
        if args.week is not None and len(set(args.period)) != 1:
            raise ValueError("--week requires exactly one fiscal period")
    elif args.year is not None or args.week is not None:
        raise ValueError("--year and --week require --period")

    resources = list(SYNC_MODULES)
    args.sync = args.sync or resources
    if "all" in args.sync:
        args.sync = resources
    # Validate resource options before opening the calendar connection.
    if {"orders", "labor"}.intersection(args.sync):
        args.business_date_start = args.business_date_end = date.min
    sync_options(args)
    ranges = (fiscal_calendar.fiscal_date_ranges(args.year, args.period, args.week)
              if args.period is not None else [
                  fiscal_calendar.weekly_fiscal_date_range() if args.weekly
                  else fiscal_calendar.daily_date_range(None if args.daily is True else args.daily)
              ])
    runs = []
    for start, end in ranges:
        if {"orders", "labor"}.intersection(args.sync):
            args.business_date_start, args.business_date_end = start, end
        logger.info("Toast business dates %s through %s", start, end)
        runs.append(sync_options(args))
    return runs


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sync", "--tables", nargs="+", choices=[*SYNC_MODULES, "all"],
        help="Resources to sync in requested order (default: all resources for the mode; restaurants without a mode).",
    )
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--daily", nargs="?", const=True, type=date.fromisoformat, metavar="YYYY-MM-DD",
                       help="Sync one date (default: yesterday).")
    modes.add_argument("--weekly", action="store_true", help="Sync the last completed fiscal week.")
    modes.add_argument("--period", type=positive_integer, nargs="+", help="Sync one or more fiscal periods from core.calendar.")
    parser.add_argument("--year", type=positive_integer, help="Fiscal year; required with --period.")
    parser.add_argument("--week", type=positive_integer, help="Limit a single fiscal period to this fiscal week.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and validate without database writes.")
    dates.add_arguments(parser)
    for module in SYNC_MODULES.values():
        if hasattr(module, "add_arguments"):
            module.add_arguments(parser)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        runs = prepare_runs(args)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        locations = get_configured_restaurants()
        if not locations:
            logger.info("No configured Toast restaurant GUIDs; selected syncs skipped")
            return 0
        client = ToastClient(load_locations=False)
        if not client.get_access_token():
            raise RuntimeError("Toast authentication failed")
    except Exception as exc:
        logger.error("Toast sync initialization failed (%s)", type(exc).__name__)
        return 1

    failed = False
    for resource in dict.fromkeys(args.sync):
        for index, options in enumerate(runs[:1] if resource in {"restaurants", "configuration"} else runs):
            resource_options = options[resource].copy()
            if resource == "labor" and index:
                resource_options["resources"] = tuple(
                    name for name in resource_options["resources"]
                    if name not in {"employees", "jobs"}
                )
                if not resource_options["resources"]:
                    continue
            logger.info("Starting Toast %s sync", resource)
            try:
                counts = SYNC_MODULES[resource].sync(
                    client, locations, dry_run=args.dry_run, **resource_options,
                )
                if counts["api_errors"] or counts["rejected"] or counts.get("database_errors", 0):
                    failed = True
            except Exception as exc:
                logger.error("Toast %s sync failed (%s)", resource, type(exc).__name__)
                failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
