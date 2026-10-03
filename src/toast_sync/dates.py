"""Business-date CLI shared by Orders and Labor."""

from datetime import date


def add_arguments(parser):
    parser.add_argument("--business-date", type=date.fromisoformat, help="One Toast business date (YYYY-MM-DD).")
    parser.add_argument("--business-date-start", type=date.fromisoformat, help="Inclusive first business date.")
    parser.add_argument("--business-date-end", type=date.fromisoformat, help="Inclusive last business date.")


def business_date_options(args, required_for=None):
    if args.business_date and (args.business_date_start or args.business_date_end):
        raise ValueError("Use --business-date or a business-date range, not both")
    start = args.business_date or args.business_date_start or args.business_date_end
    end = args.business_date or args.business_date_end or args.business_date_start
    if start is None:
        if required_for:
            raise ValueError(f"{required_for} require --business-date or --business-date-start/--business-date-end")
        return {}
    if end < start:
        raise ValueError("Business-date end must be on or after start")
    if not {"orders", "labor"}.intersection(args.sync):
        raise ValueError("Business-date arguments require --sync orders or labor")
    return {"business_date_start": start, "business_date_end": end}
