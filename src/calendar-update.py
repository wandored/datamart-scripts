"""
This script automates the creation of a new fiscal calendar for the following year
based on an existing fiscal calendar. It processes the data from a CSV file,
updates the dates, and writes the updated calendar to both a new CSV file and a PostgreSQL database.
"""

import pandas as pd
import argparse
from datetime import datetime, timedelta
from db_utils.dbconnect import DatabaseConnection
from db_utils.recreate_views import recreate_all_views


# Define a function to increment dates by 365 days
def increment_date(date_str):
    try:
        # Convert date to datetime object
        date_obj = datetime.strptime(date_str, "%m/%d/%y")
        # Add 365 days
        new_date_obj = date_obj + timedelta(days=364)
        # Convert back to string
        return new_date_obj.strftime("%m/%d/%y")
    except ValueError:
        # Handle invalid dates or non-date fields
        return date_str


def create_new_year(year):
    # Read the CSV file
    input_file = "./downloads/fiscal_calendar.csv"  # Replace with your file path
    output_file = f"./output/fiscal_calendar_{year}.csv"

    df = pd.read_csv(input_file)

    # Increment the date columns
    date_columns = [
        "date",
        "week_start",
        "week_end",
        "period_start",
        "period_end",
        "quarter_start",
        "quarter_end",
        "year_start",
        "year_end",
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].apply(increment_date)

    # change year to 2025
    df["year"] = 2026
    # Save the updated dataframe to a new CSV file
    df.to_csv(output_file, index=False)

    print(f"Updated fiscal calendar saved to {output_file}")


def write_to_database(conn, engine):
    # Read the CSV file
    input_file = "output/fiscal_calendar_2026.csv"  # Replace with your file path
    df = pd.read_csv(input_file)
    new_year = pd.read_csv(input_file)
    date_columns = [
        "date",
        "week_start",
        "week_end",
        "period_start",
        "period_end",
        "quarter_start",
        "quarter_end",
        "year_start",
        "year_end",
    ]
    # convert date_columns to date type
    for col in date_columns:
        if col in new_year.columns:
            new_year[col] = pd.to_datetime(new_year[col]).dt.strftime("%Y-%m-%d")

    # update database with table
    try:
        new_year.to_sql("calendar", engine, if_exists="append", index=False)
        conn.commit()
    except IntegrityError:
        print("Error writing to database: IntegrityError")
        return 1
    except Exception as e:
        print(f"Error writing to database: {e}")
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a new fiscal calendar for the following year."
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="The fiscal year to base the new calendar on (e.g., 2025).",
    )
    args = parser.parse_args()
    year = args.year

    with DatabaseConnection() as db:
        # create_new_year(year)
        write_to_database(db.conn, db.engine)
        # recreate_all_views(db.conn)
