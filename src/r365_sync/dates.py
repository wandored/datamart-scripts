"""R365 dates support."""

from datetime import date, datetime, timedelta


def labor_date_range(modified_on_start=None, modified_on_end=None):
    """Use today by default; one supplied boundary means that single day."""
    start = modified_on_start or modified_on_end or datetime.now().date().isoformat()
    end = modified_on_end or start
    start = date.fromisoformat(str(start))
    end = date.fromisoformat(str(end))
    if start > end:
        raise ValueError("Modified-on start must not be after modified-on end")
    return start.isoformat(), end.isoformat()


def daily_sales_date_range(business_date_start=None, business_date_end=None):
    """Default to the previous seven completed dates; one boundary means one day."""
    if business_date_start is None and business_date_end is None:
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=6)
    else:
        start = date.fromisoformat(str(business_date_start or business_date_end))
        end = date.fromisoformat(str(business_date_end or business_date_start))
    if start > end:
        raise ValueError("Business-date start must not be after business-date end")
    return start, end


def add_arguments(parser):
    parser.add_argument(
        "--modified-on-start", type=date.fromisoformat,
        help="Labor modification start date (YYYY-MM-DD); defaults to today.",
    )
    parser.add_argument(
        "--modified-on-end", type=date.fromisoformat,
        help="Labor modification end date (YYYY-MM-DD); one boundary selects a single day.",
    )
    parser.add_argument(
        "--business-date-start", type=date.fromisoformat,
        help="Daily sales start date (YYYY-MM-DD); defaults to seven days ago.",
    )
    parser.add_argument(
        "--business-date-end", type=date.fromisoformat,
        help="Daily sales end date (YYYY-MM-DD); defaults to yesterday.",
    )


def labor_options(args):
    start, end = labor_date_range(args.modified_on_start, args.modified_on_end)
    return {"modified_on_start": start, "modified_on_end": end}
