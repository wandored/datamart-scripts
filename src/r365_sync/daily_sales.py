"""R365 daily sales support."""

from datetime import date, datetime, timedelta, timezone
from uuid import UUID

import pandas as pd
from psycopg2 import sql

from db_utils.dbconnect import DatabaseConnection
from db_utils.r365_importers import get_daily_sales_pages
from src.r365_sync.dates import daily_sales_date_range
from src.r365_sync.sales_account import r365_sales_accounts
from src.r365_sync.sales_details import (
    SALES_DETAIL_GROUP_COLUMNS,
    r365_sales_detail_rows,
    r365_sales_details,
)
from src.r365_sync.sales_payments import r365_sales_payments
from src.r365_sync.sales_ticket_taxes import r365_sales_ticket_taxes
from src.r365_sync.sales_tickets import r365_sales_tickets
from src.r365_sync.values import (
    required_uuid,
    sales_array,
    sales_numeric,
    unique_r365_records,
)
from src.r365_sync.writing import write_to_db


def r365_daily_sales(pages, location_id, business_date):
    columns = [
        "id",
        "location_id",
        "business_date",
        "net_sales",
        "gross_sales",
        "guest_count",
        "total_labor_hours",
        "total_labor_amount",
        "total_labor_percentage",
        "total_deposit",
        "expected_cash",
        "paid_in_total",
        "paid_out_total",
        "over_short",
    ]
    records = []
    for row in pages:
        record = {
            "id": required_uuid(row, "id"),
            "location_id": required_uuid(row.get("location") or {}, "id"),
            "business_date": date.fromisoformat(row["businessDate"]),
            "net_sales": sales_numeric(row.get("netSales")),
            "gross_sales": sales_numeric(row.get("grossSales")),
            "guest_count": row.get("guestCount"),
            "total_labor_hours": sales_numeric(row.get("totalLaborHours")),
            "total_labor_amount": sales_numeric(row.get("totalLaborAmount")),
            "total_labor_percentage": sales_numeric(row.get("totalLaborPercentage")),
            "total_deposit": sales_numeric(row.get("totalDeposit")),
            "expected_cash": sales_numeric(row.get("expectedCash")),
            "paid_in_total": sales_numeric(row.get("paidInTotal")),
            "paid_out_total": sales_numeric(row.get("paidOutTotal")),
            "over_short": sales_numeric(row.get("overShort")),
        }
        if record["location_id"] != UUID(str(location_id)) or record[
            "business_date"
        ] != date.fromisoformat(str(business_date)):
            raise ValueError(
                "Daily sales response does not match requested location/date"
            )
        records.append(record)
    records = unique_r365_records(records)
    if len(records) != 1:
        raise ValueError(
            "Daily sales pages must contain exactly one consistent summary"
        )
    return pd.DataFrame(records, columns=columns, dtype=object)


def get_active_sales_locations():
    """Use the restaurant list, excluding inactive rows and unmapped entities."""
    with DatabaseConnection() as db:
        db.execute("""
            SELECT r365_guid AS id, timezone
            FROM core.restaurants
            WHERE active IS TRUE AND r365_guid IS NOT NULL
            ORDER BY r365_guid
        """)
        return db.fetchall()


def sync_daily_sales(
    client,
    location_ids=None,
    business_date_start=None,
    business_date_end=None,
    location_timezones=None,
):
    start, end = daily_sales_date_range(business_date_start, business_date_end)
    if location_ids is None:
        locations = get_active_sales_locations()
        location_ids = [row["id"] for row in locations]
        location_timezones = {str(row["id"]): row["timezone"] for row in locations}
    if not location_ids:
        print("No active restaurants with an R365 GUID; skipping daily sales.")
        return
    for location_id in dict.fromkeys(location_ids):
        business_date = start
        while business_date <= end:
            pages = get_daily_sales_pages(client, business_date, location_id)
            if not pages:
                print(
                    f"Skipped daily sales for {location_id} on {business_date}: "
                    "API returned 404 (missing summary or location); stored rows unchanged."
                )
                business_date += timedelta(days=1)
                continue
            summary = r365_daily_sales(pages, location_id, business_date)
            tickets = [
                ticket for page in pages for ticket in sales_array(page, "salesTickets")
            ]
            # Deduplicate complete tickets before flattening positional taxes. A changed
            # ticket across pages makes the snapshot unsafe to reconcile.
            for ticket in tickets:
                required_uuid(ticket, "id")
            tickets = unique_r365_records(tickets)
            summary_id = summary.iloc[0]["id"]
            location_timezone = (location_timezones or {}).get(str(location_id))
            detail_rows = r365_sales_detail_rows(tickets)
            frames = {
                "daily_sales": summary,
                "sales_tickets": r365_sales_tickets(
                    tickets, summary_id, location_timezone
                ),
                "sales_account": r365_sales_accounts(detail_rows),
                "sales_details": r365_sales_details(detail_rows, business_date, location_id),
                "sales_payments": r365_sales_payments(tickets, location_timezone),
                "sales_ticket_taxes": r365_sales_ticket_taxes(tickets),
            }
            synced_at = datetime.now(timezone.utc)
            for table, frame in frames.items():
                frame["last_synced_at"] = synced_at
                if table not in ("daily_sales", "sales_account"):
                    frame["is_current"] = True
            counts = {}
            with DatabaseConnection() as db:
                # The header upsert locks this summary until all children are committed.
                counts["daily_sales"] = write_to_db(
                    summary, "daily_sales", "r365", db=db
                )
                db.execute(
                    """
                    UPDATE r365.sales_details SET is_current = false
                    WHERE location_id = %s AND business_date = %s AND is_current
                    """,
                    (UUID(str(location_id)), business_date),
                )
                for table in ("sales_payments", "sales_ticket_taxes"):
                    db.execute(
                        sql.SQL("""
                        UPDATE r365.{} AS child SET is_current = false
                        FROM r365.sales_tickets AS ticket
                        WHERE child.sales_ticket_id = ticket.id
                          AND ticket.daily_sales_id = %s AND child.is_current
                    """).format(sql.Identifier(table)),
                        (summary_id,),
                    )
                db.execute(
                    """
                    UPDATE r365.sales_tickets SET is_current = false
                    WHERE daily_sales_id = %s AND is_current
                """,
                    (summary_id,),
                )
                for table, frame in frames.items():
                    if table == "daily_sales":
                        continue
                    if table == "sales_ticket_taxes":
                        keys = ("sales_ticket_id", "tax_index")
                    elif table == "sales_details":
                        keys = SALES_DETAIL_GROUP_COLUMNS
                    else:
                        keys = ("id",)
                    counts[table] = write_to_db(
                        frame, table, "r365", db=db, conflict_columns=keys
                    )
            print(
                f"Daily sales {location_id} {business_date} ({len(detail_rows):,} source item lines): "
                + ", ".join(
                    f"{table} fetched/upserted={count:,}"
                    for table, count in counts.items()
                )
            )
            business_date += timedelta(days=1)


# Uniform entry point used by the command-line dispatcher.
sync = sync_daily_sales


def get_options(args):
    start, end = daily_sales_date_range(args.business_date_start, args.business_date_end)
    return {"business_date_start": start, "business_date_end": end}
