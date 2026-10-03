"""Retrieve, normalize, and upsert Toast restaurant source records."""

from datetime import datetime
import logging
from uuid import UUID

import requests

from src.toast_sync.writing import upsert_rows

logger = logging.getLogger(__name__)

RESTAURANT_COLUMNS = (
    "id", "name", "location_name", "location_code", "timezone", "closeout_hour",
    "management_group_id", "currency_code", "first_business_date", "archived",
)


def normalize_restaurant(payload, requested_guid):
    """Map RestaurantInfo.guid and general fields without reporting transforms.

    https://doc.toasttab.com/openapi/restaurants/tag/Data-definitions/schema/General/
    Missing optional fields remain SQL NULL; invalid supplied values reject the row.
    """
    if not isinstance(payload, dict) or not isinstance(payload.get("general"), dict):
        raise ValueError("Expected a RestaurantInfo object with a general object")
    try:
        restaurant_id = UUID(str(payload["guid"]))
    except (KeyError, ValueError, TypeError, AttributeError):
        raise ValueError("Missing or invalid restaurant guid") from None
    if restaurant_id != UUID(str(requested_guid)):
        raise ValueError("Returned restaurant guid differs from requested guid")

    general = payload["general"]
    row = {"id": restaurant_id}
    for column, field in (
        ("name", "name"), ("location_name", "locationName"),
        ("location_code", "locationCode"), ("timezone", "timeZone"),
        ("currency_code", "currencyCode"),
    ):
        value = general.get(field)
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{field} must be a string or null")
        row[column] = value

    closeout = general.get("closeoutHour")
    if closeout is not None and (type(closeout) is not int or not 0 <= closeout <= 12):
        raise ValueError("closeoutHour must be an integer from 0 through 12 or null")
    row["closeout_hour"] = closeout

    group = general.get("managementGroupGuid")
    try:
        row["management_group_id"] = UUID(str(group)) if group is not None else None
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Invalid managementGroupGuid") from None

    first_date = general.get("firstBusinessDate")
    row["first_business_date"] = None
    if first_date is not None:
        if type(first_date) is not int or len(str(first_date)) != 8:
            raise ValueError("firstBusinessDate must be an integer yyyyMMdd or null")
        try:
            row["first_business_date"] = datetime.strptime(str(first_date), "%Y%m%d").date()
        except ValueError:
            raise ValueError("Invalid firstBusinessDate calendar date") from None

    archived = general.get("archived")
    if archived is not None and type(archived) is not bool:
        raise ValueError("archived must be a boolean or null")
    row["archived"] = archived
    return row


def upsert_restaurants(rows):
    """Bulk upsert in one transaction; return counts only after commit succeeds."""
    return upsert_rows(rows, "restaurants", RESTAURANT_COLUMNS)


def sync_restaurants(client, locations, dry_run=False):
    """Continue API failures per location; atomically write all validated rows."""
    rows = []
    counts = {"requested": len(locations), "returned": 0, "rejected": 0,
              "api_errors": 0, "inserted": 0, "updated": 0}
    logger.info("Restaurants requested=%d", counts["requested"])
    for location in locations:
        guid = str(location["toast_guid"])
        try:
            payload = client.get_restaurant(guid)
        except (requests.RequestException, ValueError) as exc:
            counts["api_errors"] += 1
            response = getattr(exc, "response", None)
            logger.error(
                "Restaurant company_id=%s guid=%s API error=%s status=%s",
                location["id"], guid, type(exc).__name__,
                response.status_code if response is not None else "unavailable",
            )
            continue
        counts["returned"] += 1
        try:
            rows.append(normalize_restaurant(payload, guid))
        except ValueError as exc:
            counts["rejected"] += 1
            logger.error("Restaurant company_id=%s guid=%s rejected: %s", location["id"], guid, exc)

    logger.info("Restaurants returned=%d valid=%d rejected=%d API errors=%d",
                counts["returned"], len(rows), counts["rejected"], counts["api_errors"])
    if dry_run:
        logger.info("Dry run: validated %d rows; inserted=0 updated=0", len(rows))
    else:
        try:
            counts.update(upsert_restaurants(rows))
        except Exception as exc:
            # Do not log connection strings, row contents, or server exception text.
            logger.error("Restaurant batch database error=%s SQLSTATE=%s; transaction not confirmed",
                         type(exc).__name__, getattr(exc, "pgcode", None))
            raise
        logger.info("Restaurants inserted=%d updated=%d", counts["inserted"], counts["updated"])
    return counts


# Common entry point used by the module dispatcher.
sync = sync_restaurants
