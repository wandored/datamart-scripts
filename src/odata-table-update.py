import json
import logging
from io import StringIO

import pandas as pd
import requests
from psycopg2 import sql
from psycopg2.errors import UniqueViolation

from db_utils.config import Config
from db_utils.dbconnect import DatabaseConnection
from db_utils.recreate_views import recreate_all_views


def make_HTTP_request(url):
    all_records = []
    while True:
        if not url:
            break
        r = requests.get(url, auth=(Config.SRVC_USER, Config.SRVC_PSWRD))
        if r.status_code == 200:
            json_data = json.loads(r.text)
            all_records = all_records + json_data["value"]
            if "@odata.nextLink" in json_data:
                url = json_data["@odata.nextLink"]
            else:
                break
    jStr = StringIO(json.dumps(all_records))
    df = pd.read_json(jStr)
    return df


def update_glaccount(db):
    query = "$select=glAccountId,name,glAccountNumber,glType&{}"
    url = "{}/GlAccount?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        logging.warning("No data returned for GlAccount")
        return 1

    df = df.rename(
        columns={
            "glAccountId": "glaccountid",
            "glAccountNumber": "glaccountnumber",
            "glType": "gltype",
        }
    )
    df = df.astype(str).replace("nan", None)
    df = df.drop_duplicates(subset=["glaccountid"], keep="last")
    records = df[["glaccountid", "name", "glaccountnumber", "gltype"]].values.tolist()

    try:
        db.executemany(
            """
            INSERT INTO glaccount (glaccountid, name, glaccountnumber, gltype)
            VALUES %s
            ON CONFLICT (glaccountid) DO UPDATE
            SET name = EXCLUDED.name,
                glaccountnumber = EXCLUDED.glaccountnumber,
                gltype = EXCLUDED.gltype
            """,
            records,
        )
        logging.info("GlAccount table updated successfully")
        return 0
    except Exception as e:
        logging.error("Error writing to database: %s", e)
        return 1


def update_jobtitle(db):
    query = "$select=jobTitleId,name,jobCode,glAccount_Id,location_Id&{}"
    url = "{}/JobTitle?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        logging.warning("No data returned for JobTitle")
        return 1
    df = df.rename(
        columns={
            "jobTitleId": "jobtitleid",
            "jobCode": "jobcode",
            "glAccount_Id": "glaccount_id",
            "location_Id": "location_id",
        }
    )
    df = df.astype(str).replace("nan", None)
    df = df.drop_duplicates(subset=["jobtitleid"], keep="last")
    # validate foreign keys against existing glaccounts and locations, set to None if not valid
    db.execute("SELECT glaccountid FROM glaccount")
    valid_glaccounts = {row[0] for row in db.fetchall()}
    db.execute("SELECT locationid FROM location")
    valid_locations = {row[0] for row in db.fetchall()}
    df.loc[~df["glaccount_id"].isin(valid_glaccounts), "glaccount_id"] = None
    df.loc[~df["location_id"].isin(valid_locations), "location_id"] = None

    records = df[
        ["jobtitleid", "name", "jobcode", "glaccount_id", "location_id"]
    ].values.tolist()
    try:
        db.executemany(
            """
            INSERT INTO job_title (jobtitleid, name, jobcode, glaccount_id, location_id)
            VALUES %s
            ON CONFLICT (jobtitleid) DO UPDATE
            SET name = EXCLUDED.name,
                jobcode = EXCLUDED.jobcode,
                glaccount_id = EXCLUDED.glaccount_id,
                location_id = EXCLUDED.location_id
            """,
            records,
        )
        logging.info("JobTitle table updated successfully")
        return 0
    except Exception as e:
        logging.error("Error writing to database: %s", e)
        return 1


def update_location(db):
    query = "$select=locationId,name,locationNumber&{}"
    url = "{}/Location?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        logging.warning("No data returned for Location")
        return 1
    df = df.rename(
        columns={
            "locationId": "locationid",
            "locationNumber": "locationnumber",
        }
    )
    df = df.astype(str).replace("nan", None)
    df = df.drop_duplicates(subset=["locationid"], keep="last")
    records = df[["locationid", "name", "locationnumber"]].values.tolist()
    try:
        db.executemany(
            """
            INSERT INTO location (locationid, name, locationnumber)
            VALUES %s
            ON CONFLICT (locationid) DO UPDATE
            SET name = EXCLUDED.name,
                locationnumber = EXCLUDED.locationnumber
            """,
            records,
        )
        logging.info("Location table updated successfully")
        return 0
    except Exception as e:
        logging.error("Error writing to database: %s", e)
        return 1


def update_company(db):
    query = "$select=companyId,name&{}"
    url = "{}/Company?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        logging.warning("No data returned for Company")
        return 1
    df = df.rename(
        columns={
            "companyId": "companyid",
        }
    )
    df = df.astype(str).replace("nan", None)
    df = df.drop_duplicates(subset=["companyid"], keep="last")
    records = df[["companyid", "name"]].values.tolist()
    try:
        db.executemany(
            """
            INSERT INTO company (companyid, name)
            VALUES %s
            ON CONFLICT (companyid) DO UPDATE
            SET name = EXCLUDED.name
            """,
            records,
        )
        logging.info("Company table updated successfully")
        return 0
    except Exception as e:
        logging.error("Error writing to database: %s", e)
        return 1


def update_item(db):
    query = "$select=itemId,name,category1,category2,category3&{}"
    url = "{}/Item?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        logging.warning("No data returned for Item")
        return 1
    df = df.rename(
        columns={
            "itemId": "itemid",
        }
    )
    df = df.astype(str).replace("nan", None)
    df = df.drop_duplicates(subset=["itemid"], keep="last")
    records = df[
        ["itemid", "name", "category1", "category2", "category3"]
    ].values.tolist()
    try:
        db.executemany(
            """
            INSERT INTO item (itemid, name, category1, category2, category3)
            VALUES %s
            ON CONFLICT (itemid) DO UPDATE
            SET name = EXCLUDED.name,
                category1 = EXCLUDED.category1,
                category2 = EXCLUDED.category2,
                category3 = EXCLUDED.category3
            """,
            records,
        )
        logging.info("Item table updated successfully")
        return 0
    except Exception as e:
        logging.error("Error writing to database: %s", e)
        return 1


if __name__ == "__main__":
    with DatabaseConnection() as db:
        update_glaccount(db)
        update_jobtitle(db)
        update_location(db)
        update_company(db)
        update_item(db)
