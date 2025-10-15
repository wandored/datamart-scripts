import json

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
    TOKEN_CACHE_FILE = config.get("TOKEN_CACHE_FILE")
    LOCATION_GUID_LIST = config.get("LOCATION_GUID_LIST")
    MANAGEMENT_GROUP_GUID = config.get("MANAGEMENT_GROUP_GUID")
    TOAST_RESTAURANT_EXTERNAL_ID = config.get("TOAST_RESTAURANT_EXTERNAL_ID")
    LOCATION_DROP_LIST = config.get("LOCATION_DROP_LIST")
