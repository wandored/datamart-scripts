"""Transactional GUID-based bulk upserts for Toast source modules."""

from contextlib import nullcontext

from psycopg2 import sql

from db_utils.dbconnect import DatabaseConnection


def upsert_rows(rows, table, columns, db=None, conflict_columns=("id",)):
    """Bulk upsert; a supplied connection's caller owns commit and count reporting."""
    counts = {"inserted": 0, "updated": 0}
    if not rows:
        return counts
    query = sql.SQL("""
        INSERT INTO {} ({}) VALUES %s
        ON CONFLICT ({}) DO UPDATE SET {}
        RETURNING (xmax = 0) AS inserted
    """).format(
        sql.Identifier("toast", table),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        sql.SQL(", ").join(sql.Identifier(column) for column in conflict_columns),
        sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column))
            for column in columns if column not in conflict_columns
        ),
    )
    with DatabaseConnection() if db is None else nullcontext(db) as db:
        query = query.as_string(db.conn)
        for offset in range(0, len(rows), 100):
            values = [tuple(row[column] for column in columns) for row in rows[offset:offset + 100]]
            # Keep every RETURNING result by executing one execute_values page at a time.
            db.executemany(query, values, page_size=len(values))
            for result in db.fetchall():
                counts["inserted" if result["inserted"] else "updated"] += 1
    return counts
