"""Dispatch normalized Toast source syncs for restaurants, orders, and labor.

Apply the SQL for each selected resource first. See docs/toast-api-update.md.
"""

import argparse
import logging

from db_utils.toast_utils import ToastClient
from src.toast_sync.locations import get_configured_restaurants
from src.toast_sync import dates, labor, orders, restaurants

logger = logging.getLogger(__name__)

# Each module exposes sync(client, locations, dry_run=False, **resource_options).
# Optional add_arguments/get_options hooks keep resource-specific CLI logic local.
SYNC_MODULES = {
    "restaurants": restaurants,
    "orders": orders,
    "labor": labor,
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sync", nargs="+", choices=list(SYNC_MODULES), default=["restaurants"],
        help="Resources to sync, in the requested order (default: restaurants).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch and validate without database writes.")
    dates.add_arguments(parser)
    for module in SYNC_MODULES.values():
        if hasattr(module, "add_arguments"):
            module.add_arguments(parser)
    args = parser.parse_args(argv)
    try:
        dates.business_date_options(args)
        options = {
            name: module.get_options(args) if hasattr(module, "get_options") else {}
            for name, module in SYNC_MODULES.items()
        }
    except ValueError as exc:
        parser.error(str(exc))
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
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
        logger.info("Starting Toast %s sync", resource)
        try:
            counts = SYNC_MODULES[resource].sync(
                client, locations, dry_run=args.dry_run, **options[resource],
            )
            if counts["api_errors"] or counts["rejected"] or counts.get("database_errors", 0):
                failed = True
        except Exception as exc:
            logger.error("Toast %s sync failed (%s)", resource, type(exc).__name__)
            failed = True
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
