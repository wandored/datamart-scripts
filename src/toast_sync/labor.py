"""Toast Labor v1 source syncs. See docs/toast-labor.md for API window semantics."""

from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
import logging
from zoneinfo import ZoneInfo

import requests

from db_utils.dbconnect import DatabaseConnection
from src.toast_sync.dates import business_date_options
from src.toast_sync.values import (
    source_array, source_numeric, source_timestamp, source_uuid, source_value,
)
from src.toast_sync.writing import upsert_rows

logger = logging.getLogger(__name__)
ENDPOINTS = {
    "employees": "/labor/v1/employees", "jobs": "/labor/v1/jobs",
    "time_entries": "/labor/v1/timeEntries", "shifts": "/labor/v1/shifts",
}
COMMON_FIELDS = {
    "text": {"external_id": "externalId"},
    "timestamp": {"created_date": "createdDate", "modified_date": "modifiedDate", "deleted_date": "deletedDate"},
    "boolean": {"deleted": "deleted"},
}
FIELDS = {
    "employees": {
        "text": {"external_employee_id": "externalEmployeeId", "first_name": "firstName", "chosen_name": "chosenName", "last_name": "lastName", "email": "email", "phone_number": "phoneNumber", "phone_number_country_code": "phoneNumberCountryCode"},
        "uuid": {"v2_employee_guid": "v2EmployeeGuid"},
    },
    "jobs": {
        "text": {"title": "title", "code": "code", "wage_frequency": "wageFrequency"},
        "numeric": {"default_wage": "defaultWage"},
        "boolean": {"tipped": "tipped", "exclude_from_reporting": "excludeFromReporting"},
    },
    "time_entries": {
        "uuid": {"employee_id": "employeeReference.guid", "job_id": "jobReference.guid"},
        "timestamp": {"in_date": "inDate", "out_date": "outDate"},
        "boolean": {"auto_clocked_out": "autoClockedOut"},
        "numeric": {
            "regular_hours": "regularHours", "overtime_hours": "overtimeHours", "hourly_wage": "hourlyWage",
            "declared_cash_tips": "declaredCashTips", "non_cash_tips": "nonCashTips",
            "non_cash_tips_rounding_loss": "nonCashTipsRoundingLoss",
            "cash_gratuity_service_charges": "cashGratuityServiceCharges",
            "non_cash_gratuity_service_charges": "nonCashGratuityServiceCharges",
            "tips_withheld": "tipsWithheld", "cash_sales": "cashSales", "non_cash_sales": "nonCashSales",
        },
    },
    "shifts": {
        "uuid": {"employee_id": "employeeReference.guid", "job_id": "jobReference.guid", "schedule_config_id": "scheduleConfig.guid"},
        "timestamp": {"in_date": "inDate", "out_date": "outDate"},
        "numeric": {"min_before_clock_in": "scheduleConfig.minBeforeClockIn", "min_after_clock_in": "scheduleConfig.minAfterClockIn", "min_before_clock_out": "scheduleConfig.minBeforeClockOut", "min_after_clock_out": "scheduleConfig.minAfterClockOut"},
    },
}
TABLE_COLUMNS = {
    table: ("id", "restaurant_id", *(col for group in COMMON_FIELDS.values() for col in group),
            *(col for group in fields.values() for col in group),
            *(("shift_id", "business_date") if table == "time_entries" else ()), "last_synced_at")
    for table, fields in FIELDS.items()
}
TABLE_COLUMNS.update({
    "employee_jobs": ("employee_id", "job_id", "restaurant_id", "job_external_id", "last_synced_at"),
    "employee_wage_overrides": ("employee_id", "job_id", "restaurant_id", "job_external_id", "wage", "last_synced_at"),
})
RELATION_KEYS = {table: ("employee_id", "job_id") for table in ("employee_jobs", "employee_wage_overrides")}
# Strictly below both the reference's one-month and the guide's 30-day limits.
MAX_WINDOW = timedelta(days=27)


def add_arguments(parser):
    parser.add_argument("--labor-resources", nargs="+", choices=("employees", "jobs", "time-entries", "shifts"),
                        help="Labor endpoints to sync (default: all four).")
    parser.add_argument("--labor-modified-start", help="Time entries: inclusive modification timestamp with UTC offset.")
    parser.add_argument("--labor-modified-end", help="Time entries: exclusive modification timestamp with UTC offset.")


def get_options(args):
    if "labor" not in args.sync:
        if args.labor_resources or args.labor_modified_start or args.labor_modified_end:
            raise ValueError("Labor arguments require --sync labor")
        return {}
    resources = tuple(dict.fromkeys(r.replace("-", "_") for r in (args.labor_resources or ENDPOINTS)))
    options = business_date_options(args)
    modified = (args.labor_modified_start, args.labor_modified_end)
    if any(modified):
        if not all(modified) or "time_entries" not in resources:
            raise ValueError("Both Labor modification timestamps and the time-entries resource are required")
        start = source_timestamp(modified[0], "labor-modified-start")
        end = source_timestamp(modified[1], "labor-modified-end")
        if end <= start:
            raise ValueError("Labor modification end must be after start")
        options.update(modified_start=start, modified_end=end)
    needs_dates = "shifts" in resources or ("time_entries" in resources and not any(modified))
    if needs_dates and not options.get("business_date_start"):
        raise ValueError("Labor shifts and time entries without a modification window require business dates")
    return {"resources": resources, **options}


def normalize_record(table, payload, restaurant_id, synced_at=None):
    """Whitelist fields; never persist/log passcodes or entire employee payloads."""
    if not isinstance(payload, dict):
        raise ValueError("Expected an object")
    row = {"id": source_uuid(payload.get("guid"), "guid", required=True),
           "restaurant_id": source_uuid(restaurant_id, "restaurant_id", required=True),
           "last_synced_at": synced_at or datetime.now(timezone.utc)}
    converters = {"uuid": source_uuid, "timestamp": source_timestamp, "numeric": source_numeric}
    for groups in (COMMON_FIELDS, FIELDS[table]):
        for kind, fields in groups.items():
            for column, field in fields.items():
                value = source_value(payload, field)
                if kind in converters:
                    value = converters[kind](value, field)
                elif value is not None and type(value) is not {"text": str, "boolean": bool}[kind]:
                    raise ValueError(f"{field} must be {kind} or null")
                row[column] = value
    return row


def normalize_employee(payload, restaurant_id, synced_at=None):
    return normalize_record("employees", payload, restaurant_id, synced_at)


def normalize_job(payload, restaurant_id, synced_at=None):
    return normalize_record("jobs", payload, restaurant_id, synced_at)


def normalize_time_entry(payload, restaurant_id, synced_at=None):
    row = normalize_record("time_entries", payload, restaurant_id, synced_at)
    business_date = payload.get("businessDate")
    if business_date is not None:
        if not isinstance(business_date, str) or len(business_date) != 8 or not business_date.isdecimal():
            raise ValueError("businessDate must be a yyyyMMdd string or null")
        try:
            business_date = datetime.strptime(business_date, "%Y%m%d").date()
        except ValueError:
            raise ValueError("businessDate must be a valid calendar date") from None
    row["business_date"] = business_date  # Never infer a missing source business date.
    reference = payload.get("shiftReference")
    # Current reference uses an object; Toast's time-entry guide also shows a GUID string.
    row["shift_id"] = source_uuid(reference if isinstance(reference, str) else source_value(payload, "shiftReference.guid"), "shiftReference.guid")
    return row


def normalize_shift(payload, restaurant_id, synced_at=None):
    return normalize_record("shifts", payload, restaurant_id, synced_at)


def normalize_employee_relationships(payload, employee):
    tables = {"employee_jobs": [], "employee_wage_overrides": []}
    for field, table in (("jobReferences", "employee_jobs"), ("wageOverrides", "employee_wage_overrides")):
        for item in source_array(payload, field):
            if not isinstance(item, dict):
                raise ValueError(f"{field} requires objects")
            reference = item if field == "jobReferences" else item.get("jobReference")
            if not isinstance(reference, dict):
                raise ValueError(f"{field}.jobReference requires an object")
            external_id = reference.get("externalId")
            if external_id is not None and not isinstance(external_id, str):
                raise ValueError(f"{field}.externalId must be text or null")
            row = {"employee_id": employee["id"], "restaurant_id": employee["restaurant_id"],
                   "job_id": source_uuid(reference.get("guid"), f"{field}.guid", required=True),
                   "job_external_id": external_id, "last_synced_at": employee["last_synced_at"]}
            if table == "employee_wage_overrides":
                row["wage"] = source_numeric(item.get("wage"), "wageOverrides.wage")
            tables[table].append(row)
    return tables


def normalize_batch(resource, payload, restaurant_id, synced_at=None):
    """Reject a malformed response atomically; errors identify only safe record IDs."""
    tables = {resource: []}
    if resource == "employees":
        tables.update(employee_jobs=[], employee_wage_overrides=[])
    synced_at = synced_at or datetime.now(timezone.utc)
    normalizer = {"employees": normalize_employee, "jobs": normalize_job,
                  "time_entries": normalize_time_entry, "shifts": normalize_shift}[resource]
    for item in payload:
        try:
            row = normalizer(item, restaurant_id, synced_at)
            tables[resource].append(row)
            if resource == "employees":
                for table, rows in normalize_employee_relationships(item, row).items():
                    tables[table].extend(rows)
        except ValueError as exc:
            try:
                record_id = source_uuid(item.get("guid"), "guid", required=True)
            except (ValueError, AttributeError):
                record_id = "missing/invalid"
            raise ValueError(f"record={record_id}: {exc}") from None
    for table, rows in tables.items():
        unique = {}
        for row in rows:
            key = tuple(row[col] for col in RELATION_KEYS.get(table, ("id",)))
            if key in unique and row != unique[key]:
                raise ValueError(f"Conflicting {table} key={key}")
            unique[key] = row
        tables[table] = list(unique.values())
    return tables


def fetch_resource(client, resource, restaurant_id, params=None):
    """Labor v1 GET collections are arrays, with no documented pagination controls.

    employeeIds/jobIds/timeEntryIds/shiftIds limits are ID-filter limits, not pages.
    Reuse the existing restaurant-scoped client for authentication/retries/timeouts.
    """
    response = client.request(ENDPOINTS[resource], restaurant_id, params=params)
    payload = response.json(parse_float=Decimal)
    if not isinstance(payload, list):
        raise ValueError("Expected a Labor response array")
    return payload


def fetch_employees(client, restaurant_id):
    return fetch_resource(client, "employees", restaurant_id)


def fetch_jobs(client, restaurant_id):
    return fetch_resource(client, "jobs", restaurant_id)


def fetch_time_entries(client, restaurant_id, params):
    return fetch_resource(client, "time_entries", restaurant_id, params)


def fetch_shifts(client, restaurant_id, params):
    return fetch_resource(client, "shifts", restaurant_id, params)


def load_restaurant_metadata(locations):
    """Keep core's established scope; Toast supplies timezone and closeout metadata."""
    guids = [source_uuid(loc["toast_guid"], "restaurant_id", required=True) for loc in locations]
    with DatabaseConnection() as db:
        db.execute("SELECT id, timezone, closeout_hour FROM toast.restaurants WHERE id = ANY(%s::uuid[])", (guids,))
        return {str(row["id"]): dict(row) for row in db.fetchall()}


def business_window(business_date, restaurant):
    """Interpret local business boundaries only, never naive API timestamps.

    Toast source timestamps represent instants (UTC/explicit offsets). ZoneInfo is
    used for query boundaries, including DST; ambiguous/missing boundaries fail.
    """
    hour = restaurant.get("closeout_hour")
    zone = restaurant.get("timezone")
    if type(hour) is not int or not 0 <= hour <= 23 or not isinstance(zone, str):
        raise ValueError("Restaurant requires timezone and integer closeout_hour in 0..23")
    try:
        tz = ZoneInfo(zone)
    except (KeyError, ValueError):
        raise ValueError("Restaurant timezone is not a recognized IANA zone") from None
    result = []
    for day in (business_date, business_date + timedelta(days=1)):
        local = datetime.combine(day, time(hour), tzinfo=tz)
        if local.utcoffset() != local.replace(fold=1).utcoffset() or local.astimezone(timezone.utc).astimezone(tz) != local:
            raise ValueError("Restaurant closeout boundary is ambiguous or nonexistent on this date")
        result.append(local.astimezone(timezone.utc))
    return tuple(result)


def request_windows(resource, restaurant=None, *, business_date_start=None, business_date_end=None,
                    modified_start=None, modified_end=None):
    """Yield (parameters, local selection window, log label). Windows are [start,end)."""
    if resource in ("employees", "jobs"):
        yield None, None, "complete list"
        return
    if resource == "time_entries" and modified_start is not None:
        if modified_start.tzinfo is None or modified_end is None or modified_end.tzinfo is None or modified_end <= modified_start:
            raise ValueError("A valid offset-aware modification window is required")
        start, stop = modified_start.astimezone(timezone.utc), modified_end.astimezone(timezone.utc)
        while start < stop:
            end = min(start + MAX_WINDOW, stop)
            yield {"modifiedStartDate": start.isoformat(), "modifiedEndDate": end.isoformat()}, None, f"modified=[{start.isoformat()},{end.isoformat()})"
            start = end
        return
    if business_date_start is None or business_date_end is None or business_date_end < business_date_start:
        raise ValueError("A valid business-date range is required")
    for offset in range((business_date_end - business_date_start).days + 1):
        day = business_date_start + timedelta(days=offset)
        start, end = business_window(day, restaurant or {})
        # Shifts GET requires BOTH endpoints inside its window. Look ahead to keep
        # overnight shifts, then filter start times locally. >=27-day shifts cannot
        # be covered reliably by this API window; see documented limitation.
        request_end = start + MAX_WINDOW if resource == "shifts" else end
        params = {"startDate": start.isoformat(), "endDate": request_end.isoformat()}
        if resource == "time_entries":
            # businessDate= ignores includeArchived! Equivalent clock-in boundaries
            # preserve archived entries. Break details are outside this sync's scope.
            params.update(includeArchived="true")
        yield params, ((start, end) if resource == "shifts" else None), f"business_date={day}"


def select_shifts(payload, window):
    """Keep shifts starting in the requested restaurant day, including overnight."""
    start, end = window
    selected = []
    for item in payload:
        try:
            stamp = source_timestamp(item.get("inDate"), "inDate")
            if stamp is None:
                raise ValueError("inDate is required for shift date selection")
        except (AttributeError, ValueError) as exc:
            try:
                guid = source_uuid(item.get("guid"), "guid", required=True)
            except (AttributeError, ValueError):
                guid = "missing/invalid"
            raise ValueError(f"shift={guid}: invalid/missing inDate for date selection") from exc
        if start <= stamp < end:
            selected.append(item)
    return selected


def upsert_labor_tables(tables):
    """One transaction per endpoint/window, including its child relationships."""
    with DatabaseConnection() as db:
        return {table: upsert_rows(rows, table, TABLE_COLUMNS[table], db=db,
                                  conflict_columns=RELATION_KEYS.get(table, ("id",)))
                for table, rows in tables.items()}


def sync_labor(client, locations, dry_run=False, *, resources=tuple(ENDPOINTS),
               business_date_start=None, business_date_end=None, modified_start=None, modified_end=None):
    counts = {"api_errors": 0, "database_errors": 0, "rejected": 0, "restaurants_processed": 0,
              "tables": {table: {"fetched": 0, "selected": 0, "inserted": 0, "updated": 0} for table in TABLE_COLUMNS}}
    needs_metadata = "shifts" in resources or ("time_entries" in resources and modified_start is None)
    metadata, metadata_failed = {}, False
    if needs_metadata:
        try:
            metadata = load_restaurant_metadata(locations)
        except Exception as exc:
            metadata_failed = True
            counts["database_errors"] += 1
            logger.error("Labor restaurant metadata database error=%s SQLSTATE=%s", type(exc).__name__, getattr(exc, "pgcode", None))
    fetchers = {"employees": fetch_employees, "jobs": fetch_jobs, "time_entries": fetch_time_entries, "shifts": fetch_shifts}
    for location in locations:
        guid = str(location["toast_guid"])
        succeeded = False
        for resource in dict.fromkeys(resources):
            context = f"endpoint={ENDPOINTS[resource]} restaurant={guid} dates={business_date_start}..{business_date_end}"
            if metadata_failed and (resource == "shifts" or (resource == "time_entries" and modified_start is None)):
                logger.error("%s skipped: restaurant metadata unavailable", context)
                continue
            try:
                windows = request_windows(resource, metadata.get(guid), business_date_start=business_date_start,
                                          business_date_end=business_date_end, modified_start=modified_start, modified_end=modified_end)
                for params, selection_window, label in windows:
                    context = f"endpoint={ENDPOINTS[resource]} restaurant={guid} {label}"
                    try:
                        payload = fetchers[resource](client, guid) if params is None else fetchers[resource](client, guid, params)
                    except (requests.RequestException, ValueError) as exc:
                        counts["api_errors"] += 1
                        response = getattr(exc, "response", None)
                        logger.error("%s API error=%s HTTP=%s", context, type(exc).__name__, response.status_code if response is not None else "unavailable")
                        continue
                    counts["tables"][resource]["fetched"] += len(payload)
                    try:
                        selected = select_shifts(payload, selection_window) if selection_window else payload
                        tables = normalize_batch(resource, selected, guid)
                    except ValueError as exc:
                        counts["rejected"] += max(1, len(payload))
                        logger.error("%s rejected batch: %s", context, exc)
                        continue
                    for table, rows in tables.items():
                        counts["tables"][table]["selected"] += len(rows)
                        if table != resource:
                            counts["tables"][table]["fetched"] += len(rows)
                    logger.info("%s fetched=%d selected=%d dry_run=%s", context, len(payload), len(selected), dry_run)
                    if not dry_run:
                        try:
                            written = upsert_labor_tables(tables)
                        except Exception as exc:
                            counts["database_errors"] += 1
                            logger.error("%s database error=%s SQLSTATE=%s; transaction not confirmed", context, type(exc).__name__, getattr(exc, "pgcode", None))
                            continue
                        for table, result in written.items():
                            for key in ("inserted", "updated"):
                                counts["tables"][table][key] += result[key]
                    succeeded = True
            except ValueError as exc:
                counts["rejected"] += 1
                logger.error("%s invalid request: %s", context, exc)
        counts["restaurants_processed"] += int(succeeded)
    logger.info("Toast Labor Update: restaurants attempted=%d processed=%d API_errors=%d database_errors=%d rejected=%d dry_run=%s",
                len(locations), counts["restaurants_processed"], counts["api_errors"], counts["database_errors"], counts["rejected"], dry_run)
    logger.info("%-30s %9s %9s %9s %9s", "Table", "Fetched", "Selected", "Inserted", "Updated")
    for table, result in counts["tables"].items():
        logger.info("%-30s %9d %9d %9d %9d", f"toast.{table}", *(result[k] for k in ("fetched", "selected", "inserted", "updated")))
    return counts


sync = sync_labor
