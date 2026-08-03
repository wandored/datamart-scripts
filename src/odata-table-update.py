import logging

import pandas as pd
from datetime import datetime, timedelta

from db_utils.dbconnect import DatabaseConnection
from db_utils.r365_importers import (
    get_glaccounts,
    get_jobs,
    get_locations,
    get_purchase_items,
    get_vendors,
    get_pos_mapping,
)
from db_utils.r365_utils import R365Client


def update_glaccount(db, client):
    payload = get_glaccounts(client)
    df = pd.DataFrame(
        [
            {
                "glaccountid": row["id"],
                "glaccountnumber": row["number"],
                "gltype": row["glType"],
            }
            for row in payload
        ]
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


def update_jobtitle(db, client):
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = (datetime.now()).strftime("%Y-%m-%d")
    payload = get_jobs(client, start_date, end_date)
    df = pd.DataFrame(
        [
            {
                "jobtitleid": row["id"],
                "name": row["name"],
                "jobcode": row["code"],
                "location_id": row["location"]["id"],
            }
            for row in payload
        ]
    )

    # get glAccount_id to add to table
    pos_payload = get_pos_mapping(client, "posMappingJob")
    pos_df = pd.DataFrame(
        [
            {
                "jobtitleid": row["id"],
                "glaccount_id": row["glAccount"]["id"] if row["glAccount"] else None,
            }
            for row in pos_payload
        ]
    )
    df = pd.merge(df, pos_df, how="inner", on="jobtitleid")

    if df.empty:
        logging.warning("No data returned for JobTitle")
        return 1
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


def update_location(db, client):

    payload = get_locations(client)
    df = pd.DataFrame(
        [
            {
                "locationid": row["id"],
                "name": row["name"],
                "locationnumber": row["number"],
            }
            for row in payload
        ]
    )

    if df.empty:
        logging.warning("No data returned for Location")
        return 1
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


def update_company(db, client):
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = (datetime.now()).strftime("%Y-%m-%d")
    payload = get_vendors(client, start_date, end_date)
    df = pd.DataFrame(
        [
            {
                "companyid": row["id"],
                "name": row["name"],
            }
            for row in payload
        ]
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


def update_item(db, client):
    payload = get_purchase_items(client)
    df = pd.DataFrame(
        [
            {
                "itemid": row["id"],
                "name": row["name"],
                "category1": row["itemCategory1"]["name"]
                if row["itemCategory1"]
                else None,
                "category2": row["itemCategory2"]["name"]
                if row["itemCategory2"]
                else None,
                "category3": row["itemCategory3"]["name"]
                if row["itemCategory3"]
                else None,
            }
            for row in payload
        ]
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


def update_sales_accounts(db, client):
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    payload = get_pos_mapping(client, "posMappingSalesAccount", start_date)
    df = pd.DataFrame(
        [
            {
                "sales_account_id": row["id"],
                "name": row["name"],
                "gl_account": row["glAccount"]["name"] if row["glAccount"] else None,
                "serviceType": row["serviceType"],
                "sales_category": row["category"]["name"] if row["category"] else None,
                "sales_account_type": row["type"],
            }
            for row in payload
        ]
    )
    # Split ServiceType into service_type and day_part
    df[["service_type", "day_part"]] = df["serviceType"].str.rsplit(
        " - ", n=1, expand=True
    )
    df = df.drop(columns=["serviceType"])

    df = df.astype(str).replace("nan", None)
    df = df.drop_duplicates(subset=["sales_account_id"], keep="last")
    records = df[
        [
            "sales_account_id",
            "name",
            "sales_category",
            "gl_account",
            "sales_account_type",
            "service_type",
            "day_part",
        ]
    ].values.tolist()
    try:
        db.executemany(
            """
            INSERT INTO sales_accounts (sales_account_id, name, sales_category, gl_account, sales_account_type, service_type, day_part)
            VALUES %s
            ON CONFLICT (sales_account_id) DO UPDATE
            SET name = EXCLUDED.name,
                sales_category = EXCLUDED.sales_category,
                gl_account = EXCLUDED.gl_account,
                sales_account_type = EXCLUDED.sales_account_type,
                service_type = EXCLUDED.service_type,
                day_part = EXCLUDED.day_part
            """,
            records,
        )
        logging.info("sales_accounts table updated successfully")
        return 0
    except Exception as e:
        logging.error("Error writing to database: %s", e)
        return 1


if __name__ == "__main__":
    client = R365Client()
    with DatabaseConnection() as db:
        update_glaccount(db, client)
        update_jobtitle(db, client)
        update_location(db, client)
        update_company(db, client)
        update_item(db, client)
        update_sales_accounts(db, client)
