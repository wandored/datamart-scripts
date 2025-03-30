import glob

import pandas as pd
from psycopg2 import sql
from psycopg2.errors import IntegrityError

from dbconnect import DatabaseConnection


def read_budget_files(engine):
    """
    read the budget csv files from ./downloads/budgets/ and conctaenate them into a single dataframe
    """
    year = input("Enter the year you want to update: ")
    csv_files = glob.glob("./downloads/budgets/*.csv")
    df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)
    df["year"] = year
    df.rename(
        columns={
            "Location": "location",
            "glaccountnumber": "gl_number",
            "Account": "account_name",
            "Year": "year",
            "Period 01": "period_01",
            "Period 02": "period_02",
            "Period 03": "period_03",
            "Period 04": "period_04",
            "Period 05": "period_05",
            "Period 06": "period_06",
            "Period 07": "period_07",
            "Period 08": "period_08",
            "Period 09": "period_09",
            "Period 10": "period_10",
            "Period 11": "period_11",
            "Period 12": "period_12",
            "Period 13": "period_13",
        },
        inplace=True,
    )
    df.fillna(0, inplace=True)
    df_pivot = pd.melt(
        df,
        id_vars=["location", "gl_number", "account_name", "year"],
        var_name="period",
        value_name="amount",
    )
    df_pivot["period"] = df_pivot["period"].str.extract("(\d+)")
    df_pivot["year"] = df_pivot["year"].astype(int)
    df_pivot["period"] = df_pivot["period"].astype(int)

    # write to csv file
    df_pivot.to_csv(f"./output/{year}budgets.csv", index=False)

    return df_pivot


def write_to_database(df, engine, cur, conn):
    table_name = "budgets"
    temp_table_name = "temp_table"
    try:
        df.to_sql(temp_table_name, engine, if_exists="replace", index=False)
        update_query = sql.SQL(
            """
            INSERT INTO {table} (location, gl_number, account_name, year, period, amount)
            SELECT t.location, t.gl_number, t.account_name, t.year, t.period, t.amount
            FROM {temp_table} AS t
            ON CONFLICT (location, gl_number, account_name, year, period)
            DO UPDATE SET amount = EXCLUDED.amount
            """
        ).format(
            table=sql.Identifier(table_name),
            temp_table=sql.Identifier(temp_table_name),
        )
        cur.execute(update_query)
    except IntegrityError as e:
        print(e)
    finally:
        try:
            cur.execute("DROP TABLE IF EXISTS temp_table")
            conn.commit()
        except Exception as e:
            print("Error dropping temp table", e)
            conn.rollback()

    return


if __name__ == "__main__":
    with DatabaseConnection() as db:
        df = read_budget_files(db.engine)
        write_to_database(df, db.engine, db.cur, db.conn)
