import pandas as pd
import argparse
from db_utils.dbconnect import DatabaseConnection


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Reads CSV file and writes it to the database for a specified table"
    )
    parser.add_argument(
        "--table",
        type=str,
        required=True,
        help="The name of the table to write the data to.",
    )
    parser.add_argument(
        "--schema",
        type=str,
        required=True,
        help="The name of the schema to write the data to.",
    )
    parser.add_argument(
        "--file",
        type=str,
        required=True,
        help="The path to the CSV file to read.",
    )
    args = parser.parse_args()
    return args.table, args.schema, args.file


def write_to_database(db, df, table_name, table_schema):
    try:
        df.to_sql(
            table_name, db.engine, schema=table_schema, if_exists="append", index=False
        )
        print(f"Data written to table '{table_name}' successfully.")
    except Exception as e:
        print(f"Error writing to database: {e}")


def main():
    table_name, table_schema, csv_file = get_arguments()
    df = pd.read_csv(csv_file)
    df.info()
    with DatabaseConnection() as db:
        write_to_database(db, df, table_name, table_schema)


if __name__ == "__main__":
    main()
