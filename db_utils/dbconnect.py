import psycopg2
from psycopg2.extras import DictCursor, execute_values
from sqlalchemy import create_engine

from db_utils.config import Config


class DatabaseConnection:
    def __init__(self):
        self.connection_string = Config.SQLALCHEMY_DATABASE_URI
        self.engine = None
        self.conn = None
        self.cur = None

    def __enter__(self):
        try:
            self.engine = create_engine(self.connection_string)
            self.conn = psycopg2.connect(
                host=Config.HOST_SERVER,
                database=Config.PSYCOPG2_DATABASE,
                user=Config.PSYCOPG2_USER,
                password=Config.PSYCOPG2_PASS,
                cursor_factory=DictCursor,
            )
            self.cur = self.conn.cursor()
            return self
        except Exception as e:
            raise ConnectionError(f"Failed to connect: {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        if self.engine:
            self.engine.dispose()

    def execute(self, query: str, params: tuple = None):
        """Execute a single query with optional parameters."""
        self.cur.execute(query, params)
        self.conn.commit()

    def executemany(self, query: str, records: list):
        """Execute a query against all records using psycopg2 execute_values for performance."""
        execute_values(self.cur, query, records)
        self.conn.commit()

    def rollback(self):
        """Rollback the current transaction."""
        if self.conn:
            self.conn.rollback()

    def fetchall(self) -> list:
        """Fetch all results from the last executed query."""
        return self.cur.fetchall()

    def fetchone(self) -> dict:
        """Fetch a single result from the last executed query."""
        return self.cur.fetchone()

    def commit(self):
        """Commit the current transaction."""
        if self.conn:
            self.conn.commit()
