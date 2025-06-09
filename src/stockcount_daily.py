from datetime import date, timedelta
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

import pandas as pd

from dbconnect import DatabaseConnection


def get_location_ids(cur) -> pd.DataFrame:
    cur.execute(
        """
        SELECT id AS store_id
        FROM restaurants
        WHERE active = true AND id NOT IN (98, 99)
        ORDER BY name
        """
    )
    return [row[0] for row in cur.fetchall()]


def get_counts(cur, store_id):
    count_date = date.today() - timedelta(days=1)
    count_date_str = count_date.strftime("%Y-%m-%d")
    prev_date_str = (count_date - timedelta(days=1)).strftime("%Y-%m-%d")

    # Get ending counts for count_date
    cur.execute(
        """
        SELECT item_id, item_name, count_total
        FROM stockcount_monthly
        WHERE store_id = %s AND date = %s
        """,
        (store_id, count_date_str),
    )
    ending_rows = cur.fetchall()
    ending_df = pd.DataFrame(
        ending_rows, columns=["item_id", "item_name", "ending_count"]
    )

    # Get beginning counts (from day before)
    cur.execute(
        """
        SELECT item_name, count_total
        FROM stockcount_monthly
        WHERE store_id = %s AND date = %s
        """,
        (store_id, prev_date_str),
    )
    beginning_rows = cur.fetchall()
    beginning_df = pd.DataFrame(
        beginning_rows, columns=["item_name", "beginning_count"]
    )

    # Purchases
    cur.execute(
        """
        SELECT item, unit_count
        FROM stockcount_purchases
        WHERE store_id = %s AND date = %s
        """,
        (store_id, count_date_str),
    )
    purchases_df = pd.DataFrame(cur.fetchall(), columns=["item_name", "purchases"])

    # Sales
    cur.execute(
        """
        SELECT ingredient, SUM(count_usage)
        FROM stockcount_sales
        WHERE store_id = %s AND date = %s
        GROUP BY ingredient
        """,
        (store_id, count_date_str),
    )
    sales_df = pd.DataFrame(cur.fetchall(), columns=["item_name", "sales"])

    # Waste
    cur.execute(
        """
        SELECT item, SUM(quantity)
        FROM stockcount_waste
        WHERE store_id = %s AND date = %s
        GROUP BY item
        """,
        (store_id, count_date_str),
    )
    waste_df = pd.DataFrame(cur.fetchall(), columns=["item_name", "waste"])

    # Merge everything on item_id
    counts = (
        ending_df.merge(beginning_df, on="item_name", how="left")
        .merge(purchases_df, on="item_name", how="left")
        .merge(sales_df, on="item_name", how="left")
        .merge(waste_df, on="item_name", how="left")
    )

    # Fill missing values (if no activity for an item in purchases/sales/waste)
    # counts.fillna(
    #     {"beginning_count": 0, "purchases": 0, "sales": 0, "waste": 0}, inplace=True
    # )
    fill_cols = ["beginning_count", "purchases", "sales", "waste"]
    counts[fill_cols] = counts[fill_cols].apply(pd.to_numeric, errors="coerce")
    counts[fill_cols] = counts[fill_cols].fillna(0)

    # Calculations
    counts["theoretical_onhand"] = (
        counts["beginning_count"]
        + counts["purchases"]
        - counts["sales"]
        - counts["waste"]
    )
    counts["variance"] = counts["theoretical_onhand"] - counts["ending_count"]

    # Optional: round values if needed
    numeric_cols = [
        "beginning_count",
        "purchases",
        "sales",
        "waste",
        "theoretical_onhand",
        "ending_count",
        "variance",
    ]
    counts[numeric_cols] = counts[numeric_cols].round(2)

    # Add store_id and date for tracking
    counts["store_id"] = store_id
    counts["date"] = count_date_str

    counts.drop(columns=["item_id"], inplace=True)
    counts.sort_values(by="variance", ascending=True, inplace=True)
    column_order = [
        "item_name",
        "beginning_count",
        "purchases",
        "sales",
        "waste",
        "theoretical_onhand",
        "ending_count",
        "variance",
        "store_id",
        "date",
    ]
    counts = counts[column_order]

    return counts


def save_df_to_pdf(df, filename, title="Stock Count Report"):
    fig, ax = plt.subplots(
        figsize=(10, 0.5 + 0.25 * len(df))
    )  # Adjust height to number of rows
    ax.axis("tight")
    ax.axis("off")

    table = ax.table(
        cellText=df.values, colLabels=df.columns, cellLoc="center", loc="center"
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.2)

    plt.title(title, fontsize=12, pad=12)

    with PdfPages(filename) as pdf:
        pdf.savefig(fig, bbox_inches="tight")
        plt.close()


def main():
    with DatabaseConnection() as db:
        locations = get_location_ids(db.cur)
        for id in locations:
            df = get_counts(db.cur, id)
            pdf_path = f"output/store_{id}_report.pdf"
            save_df_to_pdf(df, pdf_path, title=f"Stock Report for Store {id}")

            # # Send email
            # store_email = get_email_for_store(store_id)  # implement this lookup
            # send_email_with_pdf(
            #     store_email,
            #     subject=f"Stock Report for Store {store_id}",
            #     body="Attached is your daily stock count report.",
            #     pdf_path=pdf_path
            # )


if __name__ == "__main__":
    main()
