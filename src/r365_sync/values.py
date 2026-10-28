"""R365 values support."""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import pandas as pd


def convert_uuid(value):
    if pd.isna(value):
        return None

    if isinstance(value, UUID):
        return value

    return UUID(str(value))


def required_uuid(row, field):
    value = convert_uuid(row.get(field))
    if value is None:
        raise ValueError(f"R365 record requires {field}")
    return value


def required_boolean(row, field):
    value = row.get(field)
    if not isinstance(value, bool):
        raise ValueError(f"R365 record requires a boolean {field}")
    return value


def reference_ids(row, field):
    """Preserve null versus empty relationship arrays and validate every ID."""
    references = row.get(field)
    if references is None:
        return None
    if not isinstance(references, list):
        raise ValueError(f"R365 {field} must be a list")
    return [required_uuid(reference, "id") for reference in references]


def unique_r365_records(records, key="id"):
    """Collapse identical records; do not silently choose conflicting values."""
    unique = {}
    for record in records:
        record_id = record[key]
        if record_id in unique and unique[record_id] != record:
            raise ValueError(f"Conflicting R365 records for {key} {record_id}")
        unique[record_id] = record
    return list(unique.values())


def sales_array(record, field):
    """Missing arrays indicate an incomplete payload; explicit null means no rows."""
    if field not in record:
        raise ValueError(f"Daily sales record is missing {field}")
    value = record[field]
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise ValueError(f"Daily sales {field} must be an array of objects or null")
    return value


def sales_numeric(value):
    if value is None:
        return None
    value = Decimal(str(value))
    if not value.is_finite():
        raise ValueError("Daily sales numeric values must be finite")
    return value


# Common US Windows timezone IDs returned by R365, using Unicode CLDR
# territory="001" mappings. IANA names continue to pass through unchanged.
# https://github.com/unicode-org/cldr/blob/main/common/supplemental/windowsZones.xml
R365_WINDOWS_TIMEZONES = {
    "Eastern Standard Time": "America/New_York",
    "Central Standard Time": "America/Chicago",
    "Mountain Standard Time": "America/Denver",
    "US Mountain Standard Time": "America/Phoenix",
    "Pacific Standard Time": "America/Los_Angeles",
    "Alaskan Standard Time": "America/Anchorage",
    "Hawaiian Standard Time": "Pacific/Honolulu",
    "UTC": "Etc/UTC",
}


def sales_timestamp(value, location_timezone=None):
    if value is None:
        return None
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        if not location_timezone:
            raise ValueError("Offset-free sales timestamps require a location timezone")
        zone = ZoneInfo(R365_WINDOWS_TIMEZONES.get(location_timezone, location_timezone))
        early = stamp.replace(tzinfo=zone, fold=0)
        late = stamp.replace(tzinfo=zone, fold=1)
        if early.utcoffset() != late.utcoffset():
            raise ValueError(
                "Ambiguous or nonexistent local sales timestamp needs an offset"
            )
        stamp = early
    return stamp.astimezone(timezone.utc)


def require_vendor_invoice_fields(row, fields, record_type):
    for field in fields:
        value = row.get(field)
        if pd.isna(value) or value == "":
            raise ValueError(f"{record_type} is missing required field {field}")
