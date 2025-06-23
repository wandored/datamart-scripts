"""
This script automates the creation of a new fiscal calendar for the following year
based on an existing fiscal calendar. It processes the data from a CSV file,
updates the dates, and writes the updated calendar to both a new CSV file and a PostgreSQL database.
"""

import pandas as pd
from datetime import datetime, timedelta
from dbconnect import DatabaseConnection
from utils import (
    recreate_stockcount_conversion_view,
    recreate_stockcount_sales_view,
    recreate_stockcount_purchases_view,
    recreate_stockcount_waste_view,
    recreate_stockcount_monthly_view,
)


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


def create_new_year():
    # Read the CSV file
    input_file = (
        "./downloads/current_fiscal_calendar.csv"  # Replace with your file path
    )
    output_file = "./output/new_fiscal_calendar.csv"

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
    df["year"] = 2025
    # Save the updated dataframe to a new CSV file
    df.to_csv(output_file, index=False)

    print(f"Updated fiscal calendar saved to {output_file}")


def write_to_database(conn, engine):
    # Read the CSV file
    input_file = "fiscal_calendar_2025.csv"  # Replace with your file path
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
    with DatabaseConnection() as db:
        create_new_year()
        write_to_database(db.conn, db.engine)
        recreate_stockcount_sales_view(db.conn)
        recreate_stockcount_waste_view(db.conn)
        recreate_stockcount_purchases_view(db.conn)
        recreate_stockcount_monthly_view(db.conn)
        recreate_stockcount_conversion_view(db.conn)
