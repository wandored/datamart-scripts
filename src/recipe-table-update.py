import pandas as pd
from db_utils.dbconnect import DatabaseConnection


def update_recipe_table(csv_path: str):
    # Step 1: Read the CSV file
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Step 2: Remove unnecessary columns
    df = df[["ID", "Name", "Reporting UofM", "Yield UofM", "Yield Qty"]]
    df = df.rename(
        columns={
            "ID": "recipe_id",
            "Name": "recipe_name",
            "Reporting UofM": "reporting_uofm",
            "Yield UofM": "yield_uofm",
            "Yield Qty": "yield_qty",
        }
    )

    # Step 3: Connect to the database
    with DatabaseConnection() as db:
        records = df[
            ["recipe_name", "reporting_uofm", "yield_uofm", "yield_qty", "recipe_id"]
        ].values.tolist()
        db.executemany(
            """
            INSERT INTO recipes (recipe_name, reporting_uofm, yield_uofm, yield_qty, recipe_id)
            VALUES %s
            ON CONFLICT (recipe_id) DO UPDATE
            SET recipe_name = EXCLUDED.recipe_name,
                reporting_uofm = EXCLUDED.reporting_uofm,
                yield_uofm = EXCLUDED.yield_uofm,
                yield_qty = EXCLUDED.yield_qty
            """,
            records,
        )


if __name__ == "__main__":
    update_recipe_table("downloads/RecipeItems.csv")
