# Import MenuItems_R367.csv, remove unnecessary columns, and update the menu_item table in the database.
import pandas as pd

from db_utils.dbconnect import DatabaseConnection


def update_menu_item_table(csv_path: str):
    # Step 1: Read the CSV file
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # drop all rows where no recipe is linked
    df = df.dropna(subset=["Recipe Id"])

    # split Name column into menu_item and concept
    df[["concept", "menu_item"]] = df["Name"].str.split(" - ", n=1, expand=True)

    # Step 2: Remove unnecessary columns
    df = df[["Id", "menu_item", "concept", "Category 1", "Category 2", "Category 3"]]
    df = df.rename(
        columns={
            "Id": "menu_item_id",
            "Category 1": "category_1",
            "Category 2": "category_2",
            "Category 3": "category_3",
        }
    )
    df.info()

    # import distinct menuitem and salesaccount from sales_detail table and merge with df

    # look for duplicates in menu_item_id
    if df["menu_item_id"].duplicated().any():
        # write out the duplicate menu_item_ids to a csv for review
        duplicates = df[df["menu_item_id"].duplicated(keep=False)]
        duplicates.to_csv("output/duplicate_menu_item_ids.csv", index=False)
        print("Warning: Duplicate menu_item_id found. Keeping the first occurrence.")
        df = df.drop_duplicates(subset=["menu_item_id"], keep="first")

    # Step 3: Connect to the database
    with DatabaseConnection() as db:
        # import distinct menuitem and salesaccount from sales_detail table and merge with df
        db.execute("SELECT DISTINCT menuitem, salesaccount FROM sales_detail")
        sales_detail = pd.DataFrame(
            db.fetchall(), columns=["menuitem", "sales_account"]
        )
        sales_detail_menu_items = set(sales_detail["menuitem"].unique())
        df = df.merge(
            sales_detail,
            left_on="menu_item",
            right_on="menuitem",
            how="inner",
        ).drop(columns=["menuitem"])
        # drop duplicates created by the merge
        df = df.drop_duplicates(subset=["menu_item_id"])

        db.execute("SELECT name, sales_account_id FROM sales_accounts")
        sales_accounts = pd.DataFrame(
            db.fetchall(), columns=["sales_account_name", "sales_account_id"]
        )
        df = df.merge(
            sales_accounts,
            left_on="sales_account",
            right_on="sales_account_name",
            how="left",
        ).drop(columns=["sales_account_name", "sales_account"])
        df = df.drop_duplicates(subset=["menu_item_id"])
        print(df)

        records = df[
            [
                "menu_item_id",
                "menu_item",
                "concept",
                "category_1",
                "category_2",
                "category_3",
                "sales_account_id",
            ]
        ].values.tolist()
        db.executemany(
            """
            INSERT INTO menu_items (menu_item_id, menu_item, concept, category_1, category_2, category_3, sales_account_id)
            VALUES %s
            ON CONFLICT (menu_item_id) DO UPDATE
            SET menu_item = EXCLUDED.menu_item,
                concept = EXCLUDED.concept,
                category_1 = EXCLUDED.category_1,
                category_2 = EXCLUDED.category_2,
                category_3 = EXCLUDED.category_3,
                sales_account_id = EXCLUDED.sales_account_id
            """,
            records,
        )


if __name__ == "__main__":
    update_menu_item_table("downloads/MenuItems_R365.csv")
