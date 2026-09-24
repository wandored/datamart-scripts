from typing import Any, List, Mapping, Optional, Sequence, Union

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor, execute_values
from psycopg2.extras import register_uuid

from db_utils.config import Config

register_uuid()


class DatabaseConnection:
    """
    Context manager that provides a psycopg2 connection + DictCursor.

    Usage:
        with DatabaseConnection() as db:
            db.execute("SELECT * FROM core.calendar")
            rows = db.fetchall()

    All database objects should use fully qualified schema names, e.g.:

        core.calendar
        toast.orders
        r365.sales
        reporting.menu_engineering

    Behavior:
    - Uses psycopg2 only.
    - Does not modify the PostgreSQL search_path.
    - Transaction is committed on successful __exit__.
    - Transaction is rolled back if an exception occurs.
    - Methods require an active context.
    """

    def __init__(self, autocommit: bool = False):
        self.autocommit = autocommit
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.cur: Optional[DictCursor] = None

    def __enter__(self) -> "DatabaseConnection":
        try:
            self.conn = psycopg2.connect(
                host=Config.HOST_SERVER,
                database=Config.PSYCOPG2_DATABASE,
                user=Config.PSYCOPG2_USER,
                password=Config.PSYCOPG2_PASS,
            )

            self.cur = self.conn.cursor(cursor_factory=DictCursor)

            if self.autocommit:
                self.conn.autocommit = True

            return self

        except Exception as e:
            try:
                if self.cur:
                    self.cur.close()
            except Exception:
                pass

            try:
                if self.conn:
                    self.conn.close()
            except Exception:
                pass

            raise ConnectionError(f"Failed to connect: {e}") from e

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.conn and not self.conn.autocommit:
                if exc_type is None:
                    self.conn.commit()
                else:
                    self.conn.rollback()

        finally:
            try:
                if self.cur:
                    self.cur.close()
            except Exception:
                pass

            try:
                if self.conn:
                    self.conn.close()
            except Exception:
                pass

    def _ensure_open(self):
        if self.conn is None or self.cur is None:
            raise RuntimeError(
                "DatabaseConnection methods must be used within a `with` context"
            )

    def execute(
        self,
        query: Union[str, sql.Composed],
        params: Optional[Sequence[Any]] = None,
        commit: bool = False,
    ):
        """
        Execute a single SQL statement.

        Example:
            db.execute(
                "SELECT * FROM core.calendar WHERE date = %s",
                (business_date,)
            )
        """
        self._ensure_open()

        self.cur.execute(query, params)

        if commit and not self.conn.autocommit:
            self.conn.commit()

    def executemany(
        self,
        query: str,
        records: Sequence[Sequence[Any]],
        page_size: int = 100,
        commit: bool = False,
    ):
        """
        Execute a bulk INSERT/UPDATE using execute_values().

        Query should contain a single %s placeholder for VALUES.

        Example:
            db.executemany(
                '''
                INSERT INTO toast.product_mix
                    (item_guid, business_date, store_id)
                VALUES %s
                ''',
                records,
            )
        """
        self._ensure_open()

        execute_values(
            self.cur,
            query,
            records,
            page_size=page_size,
        )

        if commit and not self.conn.autocommit:
            self.conn.commit()

    def fetchall(self) -> List[Mapping[str, Any]]:
        """Return all rows from the last executed query."""
        self._ensure_open()
        return list(self.cur.fetchall())

    def fetchone(self) -> Optional[Mapping[str, Any]]:
        """Return one row from the last executed query."""
        self._ensure_open()
        return self.cur.fetchone()

    def commit(self):
        """Explicitly commit the current transaction."""
        self._ensure_open()
        self.conn.commit()

    def rollback(self):
        """Explicitly roll back the current transaction."""
        self._ensure_open()
        self.conn.rollback()


# class DatabaseConnection:
#     def __init__(self, schema: str = None):
#         self.connection_string = Config.SQLALCHEMY_DATABASE_URI
#         self.schema = schema
#         self.engine = None
#         self.conn = None
#         self.cur = None
#
#     def __enter__(self):
#         try:
#             self.engine = create_engine(self.connection_string)
#             self.conn = psycopg2.connect(
#                 host=Config.HOST_SERVER,
#                 database=Config.PSYCOPG2_DATABASE,
#                 user=Config.PSYCOPG2_USER,
#                 password=Config.PSYCOPG2_PASS,
#                 cursor_factory=DictCursor,
#             )
#             self.cur = self.conn.cursor()
#             if self.schema:
#                 self.cur.execute(f"SET search_path TO {self.schema};")
#             return self
#         except Exception as e:
#             raise ConnectionError(f"Failed to connect: {str(e)}")
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         if exc_type is None:
#             # No exception, commit the transaction
#             if self.conn:
#                 self.conn.commit()
#         else:
#             # Exception occurred, rollback
#             if self.conn:
#                 self.conn.rollback()
#
#         if self.cur:
#             self.cur.close()
#         if self.conn:
#             self.conn.close()
#         if self.engine:
#             self.engine.dispose()
#
#     def execute(self, query: str, params: tuple = None):
#         """Execute a single query with optional parameters."""
#         self.cur.execute(query, params)
#         self.conn.commit()
#
#     def executemany(self, query: str, records: list):
#         """Execute a query against all records using psycopg2 execute_values for performance."""
#         execute_values(self.cur, query, records)
#         self.conn.commit()
#
#     def rollback(self):
#         """Rollback the current transaction."""
#         if self.conn:
#             self.conn.rollback()
#
#     def fetchall(self) -> list:
#         """Fetch all results from the last executed query."""
#         return self.cur.fetchall()
#
#     def fetchone(self) -> dict:
#         """Fetch a single result from the last executed query."""
#         return self.cur.fetchone()
#
#     def commit(self):
#         """Commit the current transaction."""
#         if self.conn:
#             self.conn.commit()
