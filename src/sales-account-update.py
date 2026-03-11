import pandas as pd
from db_utils.dbconnect import DatabaseConnection


def update_sales_account_table(csv_path: str):
    # Step 1: Read the CSV file
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Step 2: Remove unnecessary columns
    df = df[["SalesAccountId", "Name", "Category", "GLAccount"]]
    df = df.rename(
        columns={
            "SalesAccountId": "sales_account_id",
            "Name": "name",
            "Category": "sales_category",
            "GLAccount": "gl_account",
        }
    )

    # Step 3: Connect to the database
    with DatabaseConnection() as db:
        records = df[
            ["sales_account_id", "name", "sales_category", "gl_account"]
        ].values.tolist()
        db.executemany(
            """
            INSERT INTO sales_accounts (sales_account_id, name, sales_category, gl_account)
            VALUES %s
            ON CONFLICT (sales_account_id) DO UPDATE
            SET name = EXCLUDED.name,
                sales_category = EXCLUDED.sales_category,
                gl_account = EXCLUDED.gl_account
            """,
            records,
        )


if __name__ == "__main__":
    update_sales_account_table("downloads/SalesAccounts.csv")
