"""Fiscal date selections shared by source synchronization commands."""

from datetime import date, timedelta

import pandas as pd

from db_utils.dbconnect import DatabaseConnection


def fiscal_date_ranges(year, periods, week=None, *, connection_factory=DatabaseConnection):
    """Resolve fiscal selections to contiguous ranges without filling calendar gaps."""
    query = """
        SELECT date, period
        FROM core.calendar
        WHERE year = %s
    """
    params = [year]
    if periods is not None:
        query += " AND period = ANY(%s)"
        params.append(list(dict.fromkeys(periods)))
    if week is not None:
        query += " AND week = %s"
        params.append(week)
    query += " ORDER BY date"
    with connection_factory() as db:
        db.execute(query, tuple(params))
        rows = db.fetchall()
    if not rows:
        raise ValueError(f"No core.calendar dates for fiscal year {year}")
    missing = set(periods or []) - {int(row["period"]) for row in rows}
    if missing:
        raise ValueError(
            f"No core.calendar dates for fiscal year {year}, "
            f"period(s) {', '.join(map(str, sorted(missing)))}"
            + (f", week {week}" if week is not None else "")
        )
    dates = sorted({pd.Timestamp(row["date"]).date() for row in rows})
    ranges = []
    for day in dates:
        if ranges and day == ranges[-1][1] + timedelta(days=1):
            ranges[-1] = (ranges[-1][0], day)
        else:
            ranges.append((day, day))
    return ranges


def daily_date_range(selected_date=None, *, today=None):
    """Select exactly one day, defaulting to yesterday without a calendar query."""
    day = selected_date or (today or date.today()) - timedelta(days=1)
    return day, day


def fiscal_week_to_date(*, today=None, connection_factory=DatabaseConnection):
    """Import the fiscal week containing yesterday, through yesterday inclusive."""
    yesterday = (today or date.today()) - timedelta(days=1)
    with connection_factory() as db:
        db.execute(
            """
            SELECT date
            FROM core.calendar
            WHERE (year, period, week) = (
                SELECT year, period, week FROM core.calendar WHERE date = %s
            ) AND date <= %s
            ORDER BY date
            """,
            (yesterday, yesterday),
        )
        dates = sorted({pd.Timestamp(row["date"]).date() for row in db.fetchall()})
    if not dates or dates[-1] != yesterday:
        raise ValueError(f"No core.calendar fiscal week for yesterday ({yesterday})")
    if len(dates) != (yesterday - dates[0]).days + 1:
        raise ValueError("The fiscal week in core.calendar contains missing dates")
    return dates[0], yesterday


def weekly_fiscal_date_range(*, today=None, connection_factory=DatabaseConnection):
    """Return the complete fiscal week immediately before today's fiscal week."""
    today = today or date.today()
    # Including today identifies the start of the current fiscal week.
    current_start, _ = fiscal_week_to_date(
        today=today + timedelta(days=1), connection_factory=connection_factory,
    )
    return fiscal_week_to_date(today=current_start, connection_factory=connection_factory)
