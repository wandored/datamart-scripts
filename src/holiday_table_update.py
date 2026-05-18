import pandas as pd
from db_utils.config import Config
from db_utils.dbconnect import DatabaseConnection


CSV_FILE = "./downloads/holiday_table.csv"


def main():
    df = pd.read_csv(CSV_FILE)
    df["holiday_date"] = pd.to_datetime(df["holiday_date"], errors="coerce")

    with DatabaseConnection() as db:
        # Optional yearly cleanup
        years = pd.to_datetime(df["holiday_date"]).dt.year.unique().tolist()

        # Determine years included in file
        years = sorted(df["holiday_date"].dt.year.unique().tolist())
        print(f"Updating holiday years: {years}")

        # Delete existing entries for those years
        delete_query = f"""DELETE FROM holiday
WHERE EXTRACT(YEAR FROM holiday_date) IN ({",".join(map(str, years))})"""
        db.execute(delete_query)
        print(f"Deleted existing entries for years: {years}")

        # Insert new data
        insert_query = """INSERT INTO holiday (holiday_key, holiday_name, holiday_date, holiday_offset)
VALUES (%s, %s, %s, %s)"""
        for _, row in df.iterrows():
            db.execute(
                insert_query,
                (
                    row["holiday_key"],
                    row["holiday_name"],
                    row["holiday_date"],
                    row["holiday_offset"],
                ),
            )

    print("Holiday table updated.")


if __name__ == "__main__":
    main()
