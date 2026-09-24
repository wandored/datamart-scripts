"""
Import and normalize R365 Profit & Loss report CSV files for comparison reporting.

Each R365 weekly P&L export contains the report date, location, account/category,
amounts, percentages, and R365-calculated totals. This script extracts the useful
financial data from the export and stores it in a local SQLite data store.

"""

import re
import csv
from datetime import datetime
from pathlib import Path
import sqlite3
from db_utils.dbconnect import DatabaseConnection

# DB_PATH = Path(__file__).resolve().parent.parent / "data" / "pnl.db"


# def get_connection():
#     return sqlite3.connect(DB_PATH)


def get_calendar_data(date):
    with DatabaseConnection() as db:
        query = """
            SELECT week, period, year, week_index, period_index
            FROM core.calendar
            WHERE date = %s
        """
        db.execute(query, (date,))
        result = db.fetchone()
        if result:
            return (
                result["week"],
                result["period"],
                result["year"],
                result["week_index"],
                result["period_index"],
            )
        else:
            raise ValueError(f"No end_of_week_date found for date={date}")


def fetch_date_from_csv(csv_file_path):
    """Extract the week-ending date from the R365 P&L CSV file."""
    with open(csv_file_path, "r") as f:
        # Read the second line (index 1)
        f.readline()  # Skip header
        header_line = f.readline()

    # Extract date using regex pattern "Week Ending MM/DD/YYYY"
    match = re.search(r"Week Ending (\d{2}/\d{2}/\d{4})", header_line)
    if match:
        date_str = match.group(1)
        # Parse to date object
        date_obj = datetime.strptime(date_str, "%m/%d/%Y").date()
        date_str = date_obj.strftime("%Y-%m-%d")  # Convert to YYYY-MM-DD format
        return date_str
    else:
        raise ValueError("Could not find week-ending date in CSV header")


def read_and_clean_csv(
    csv_file_path, file_date, week, period, year, week_index, period_index
):
    """
    Read the R365 P&L CSV file, clean and normalize the data,
    and add it to dataframe with week, period, year,
    week_indexand period_index.
    """

    pl_data = []
    category_order_map = {}

    with open(csv_file_path, "r") as f:
        # Skip the first two lines (header and date line)
        for _ in range(4):
            f.readline()

        reader = csv.reader(f)
        for columns in reader:
            if len(columns) < 5:
                continue  # Skip lines that don't have enough columns

            # Extract relevant fields (assuming specific column order)
            location = columns[1].strip()
            account_category = columns[5].strip()
            amount_str = columns[6].strip()

            if not amount_str:
                continue

            # Normalize amount string:
            # - Handle parentheses for negative numbers: (1,234) -> -1234
            # - Remove commas, percent signs, and stray spaces
            if amount_str.startswith("(") and amount_str.endswith(")"):
                amount_str_clean = "-" + amount_str[1:-1]
            else:
                amount_str_clean = amount_str

            amount_str_clean = (
                amount_str_clean.replace(",", "").replace("%", "").replace(" ", "")
            )

            try:
                amount = float(amount_str_clean)
            except ValueError:
                continue

            if amount == 0.0:
                continue

            # # Assign report_order based on first appearance of account_category
            # if account_category not in category_order_map:
            #     category_order_map[account_category] = report_order
            #     report_order += 1

            # add data to pl_data list with week, period, year, week_index, and period_index
            pl_data.append(
                {
                    "week_ending": file_date,
                    "week": week,
                    "period": period,
                    "year": year,
                    "week_index": week_index,
                    "period_index": period_index,
                    "location": location,
                    "account_category": account_category,
                    "amount": amount,
                }
            )
    print(f"Processed {len(pl_data)} rows of P&L data from CSV.")
    return pl_data


def save_to_database(pl_data):

    date = pl_data[0]["week_ending"]

    if not all(row["week_ending"] == date for row in pl_data):
        raise ValueError("All rows must belong to the same date")

    with DatabaseConnection() as db:
        db.execute("DELETE FROM reporting.pnl WHERE week_ending = %s", (date,))

        db.executemany(
            """
            INSERT INTO reporting.pnl (
                week_ending,
                week,
                period,
                year,
                week_index,
                period_index,
                location,
                account_category,
                amount
            )
            VALUES %s
            """,
            [
                (
                    row["week_ending"],
                    row["week"],
                    row["period"],
                    row["year"],
                    row["week_index"],
                    row["period_index"],
                    row["location"],
                    row["account_category"],
                    row["amount"],
                )
                for row in pl_data
            ],
        )


def main():
    file_date = fetch_date_from_csv("downloads/profit_and_loss.csv")
    week, period, year, week_index, period_index = get_calendar_data(file_date)

    pl_data = read_and_clean_csv(
        "downloads/profit_and_loss.csv",
        file_date,
        week,
        period,
        year,
        week_index,
        period_index,
    )

    save_to_database(pl_data)
    print(f"Saved {len(pl_data)} rows for period {period} week {week} to the database.")


if __name__ == "__main__":
    main()
