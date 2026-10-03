"""Shared company configuration for Toast sync modules."""

from db_utils.dbconnect import DatabaseConnection


def get_configured_restaurants():
    """Company master data defines scope, including inactive mapped locations."""
    with DatabaseConnection() as db:
        db.execute("""
            SELECT id, name, toast_guid
            FROM core.restaurants
            WHERE toast_guid IS NOT NULL
            ORDER BY id
        """)
        return db.fetchall()
