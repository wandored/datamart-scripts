"""R365 writing support."""

from contextlib import nullcontext

import pandas as pd
from psycopg2 import sql

from db_utils.dbconnect import DatabaseConnection


def write_to_db(
    df: pd.DataFrame, table_name: str, schema: str, db=None, conflict_columns=("id",)
):
    """Upsert rows; a supplied connection's caller owns commit and reporting."""
    if not schema:
        raise ValueError("schema is required")

    if not table_name:
        raise ValueError("table_name is required")

    if df.empty:
        print(f"No rows to write to {schema}.{table_name}.")
        return 0

    if not conflict_columns or any(col not in df.columns for col in conflict_columns):
        raise ValueError(f"{schema}.{table_name} must contain its conflict columns")

    owns_connection = db is None
    with DatabaseConnection() if owns_connection else nullcontext(db) as db:
        table_ref = sql.Identifier(schema, table_name)

        columns = [sql.Identifier(col) for col in df.columns]

        update_columns = [col for col in df.columns if col not in conflict_columns]

        update_sql = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(col),
                sql.Identifier(col),
            )
            for col in update_columns
        )

        insert_sql = sql.SQL("""
            INSERT INTO {} ({})
            VALUES %s
            ON CONFLICT ({})
            DO UPDATE SET {}
        """).format(
            table_ref,
            sql.SQL(", ").join(columns),
            sql.SQL(", ").join(sql.Identifier(col) for col in conflict_columns),
            update_sql,
        )

        # Convert pandas NaN / NaT / pd.NA to Python None
        df = df.astype(object).where(pd.notna(df), None)

        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

        db.executemany(
            insert_sql.as_string(db.conn),
            rows,
        )

    if owns_connection:
        print(f"Upserted {len(rows):,} rows into {schema}.{table_name}.")
    return len(rows)
