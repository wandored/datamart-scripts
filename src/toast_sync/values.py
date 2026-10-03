"""Strict source scalar parsing shared by Toast endpoint modules."""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from uuid import UUID


def source_uuid(value, field, required=False):
    if value is None and not required:
        return None
    try:
        return UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        raise ValueError(f"{field} must be a valid UUID") from None


def source_timestamp(value, field):
    if value is None:
        return None
    try:
        result = datetime.fromisoformat(value)
        if result.tzinfo is None:
            raise ValueError
        return result
    except (TypeError, ValueError):
        raise ValueError(f"{field} must be an ISO timestamp with an offset or null") from None


def source_numeric(value, field):
    if value is None:
        return None
    try:
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise ValueError
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError
        return result
    except (InvalidOperation, ValueError):
        raise ValueError(f"{field} must be a finite number or null") from None


def source_value(payload, path):
    value = payload
    for part in path.split("."):
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError(f"{path} requires an object")
        value = value.get(part)
    return value


def source_array(payload, field):
    value = payload.get(field)
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field} must be an array or null")
    return value

