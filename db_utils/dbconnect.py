import psycopg2
from psycopg2.extras import DictCursor
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
        print("Database connection closed")
