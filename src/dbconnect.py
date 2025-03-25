import psycopg2
from psycopg2.extras import DictCursor

# from sqlalchemy import create_engine

from config import Config


class DatabaseConnection:
    def __init__(self):
        self.connection_string = Config.SQLALCHEMY_DATABASE_URI
        self.conn = None
        self.cur = None

    def __enter__(self):
        try:
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
        print("Database connection closed")
