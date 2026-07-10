import json
import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

with open(".env/pgdb_config.json") as config_file:
    config = json.load(config_file)


class Config:
    SQLALCHEMY_DATABASE_URI = config.get("SQLALCHEMY_DATABASE_URI")
    SQLALCHEMY_TRACK_MODIFICATIONS = config.get("SQLALCHEMY_TRACK_MODIFICATIONS")
    SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping": True}
    SQLALCHEMY_POOL_SIZE = 10
    SQLALCHEMY_MAX_OVERFLOW = 20
    SQLALCHEMY_POOL_RECYCLE = 1800

    SRVC_ROOT = config.get("SRVC_ROOT")
    SRVC_USER = config.get("SRVC_USER")
    SRVC_PSWRD = config.get("SRVC_PSWRD")
    HOST_SERVER = config.get("HOST_SERVER")
    PSYCOPG2_DATABASE = config.get("PSYCOPG2_DATABASE")
    PSYCOPG2_USER = config.get("PSYCOPG2_USER")
    PSYCOPG2_PASS = config.get("PSYCOPG2_PASS")

    # Ensure the token cache file path is absolute
    TOKEN_CACHE_FILE = config.get("TOKEN_CACHE_FILE")
    if not TOKEN_CACHE_FILE:
        TOKEN_CACHE_FILE = PROJECT_ROOT / ".env" / "token_cache.json"
    else:
        TOKEN_CACHE_FILE = Path(TOKEN_CACHE_FILE).expanduser().resolve()

    # Ensure BASE_OUTPUT_DIR is an absolute path
    BASE_OUTPUT_DIR = (
        Path(config.get("OUTPUT_DIR", PROJECT_ROOT / "output")).expanduser().resolve()
    )

    # Toast API configuration
    MANAGEMENT_GROUP_GUID = config.get("MANAGEMENT_GROUP_GUID")
    TOAST_RESTAURANT_EXTERNAL_ID = config.get("TOAST_RESTAURANT_EXTERNAL_ID")
    TOAST_API_ACCESS_URL = config.get("TOAST_API_ACCESS_URL")
    USER_ACCESS_TYPE = config.get("USER_ACCESS_TYPE")
    CLIENT_ID = config.get("CLIENT_ID")
    CLIENT_SECRET = config.get("CLIENT_SECRET")
    LOCATION_DROP_LIST = config.get("LOCATION_DROP_LIST")

    # R365 API configuration
    R365_BASE_URL = config.get("R365_BASE_URL")
    R365_TOKEN = config.get("R365_TOKEN")
    R365_SECURITY_ID = config.get("R365_SECURITY_ID")
    R365_TENANT_ID = config.get("R365_TENANT_ID")

    MAIL_USER = config.get("EMAIL_USER")
    MAIL_PASS = config.get("EMAIL_PASS")
    MAIL_SERVER = config.get("EMAIL_SERVER")
    MAIL_PORT = config.get("EMAIL_PORT")
    MAIL_USE_TLS = config.get("EMAIL_USE_TLS")
    MAIL_DEFAULT_SENDER = config.get("EMAIL_DEFAULT_SENDER")
    MAIL_STOCKCOUNT_GROUP = config.get("EMAIL_STOCKCOUNT_GROUP")
    MAIL_LIQUOR_GROUP = config.get("EMAIL_LIQUOR_GROUP")
    MAIL_WINE_GROUP = config.get("EMAIL_WINE_GROUP")
    MAIL_BEER_GROUP = config.get("EMAIL_BEER_GROUP")
