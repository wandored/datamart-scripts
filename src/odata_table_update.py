import json

import pandas as pd
import requests
from psycopg2 import sql
from psycopg2.errors import UniqueViolation
from io import StringIO


from config import Config
from dbconnect import DatabaseConnection


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


def update_glaccount(cur, conn, engine):
    query = "$select=glAccountId,name,glAccountNumber,glType&{}"
    url = "{}/GlAccount?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        return
    df = df.rename(
        columns={
            "glAccountId": "glaccountid",
            "glAccountNumber": "glaccountnumber",
            "glType": "gltype",
        }
    )
    # table_name = "glaccount"
    # temp_table_name = "temp_table"
    try:
        df.to_sql("temp_table", engine, if_exists="replace", index=False)
        upsert_query = sql.SQL(
            """
       INSERT INTO {table} (glaccountid, name, glaccountnumber, gltype)
       SELECT t.glaccountid, t.name, t.glaccountnumber, t.gltype
        FROM {temp_table} AS t
        ON CONFLICT (glaccountid) DO UPDATE
        SET name = EXCLUDED.name,
            glaccountnumber = EXCLUDED.glaccountnumber,
            glType = EXCLUDED.glType
       """
        ).format(
            table=sql.Identifier("glaccount"), temp_table=sql.Identifier("temp_table")
        )
        cur.execute(upsert_query)
    except UniqueViolation:
        print("Error upserting data due to unique contraint violation.")
        return 1
    except Exception as e:
        print("Error writing to database:", e)
        return 1
    finally:
        try:
            # drop temporary table
            cur.execute("DROP TABLE IF EXISTS {} CASCADE".format("temp_table"))
            conn.commit()
        except Exception as e:
            print("Error dropping temporary table:", e)
            conn.rollback()  # rollback the transaction if an error occurs
            return 1
    return 0


def update_jobtitle(cur, conn, engine):
    query = "$select=jobTitleId,name,jobCode,glAccount_Id,location_Id&{}"
    url = "{}/JobTitle?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        return
    df = df.rename(
        columns={
            "jobTitleId": "jobtitleid",
            "jobCode": "jobcode",
            "glAccount_Id": "glaccount_id",
            "location_Id": "location_id",
        }
    )
    # table_name = "job_title"
    # temp_table_name = "temp_table"
    try:
        df.to_sql("temp_table", engine, if_exists="replace", index=False)
        upsert_query = sql.SQL(
            """
            INSERT INTO {table} (jobtitleid, name, jobcode, glaccount_id, location_id)
            SELECT t.jobtitleid, t.name, t.jobcode, t.glaccount_id, t.location_id
            FROM {temp_table} AS t
            ON CONFLICT (jobtitleid) DO UPDATE
            SET name = EXCLUDED.name,
                jobcode = EXCLUDED.jobcode,
                glaccount_id = EXCLUDED.glaccount_id,
                location_id = EXCLUDED.location_id
            """
        ).format(
            table=sql.Identifier("job_title"), temp_table=sql.Identifier("temp_table")
        )
        cur.execute(upsert_query)
        conn.commit()
    except UniqueViolation:
        print("Error upserting data due to unique contraint violation.")
        return 1
    except Exception as e:
        print("Error writing to database:", e)
        return 1
    finally:
        try:
            # drop temporary table
            cur.execute("DROP TABLE IF EXISTS {} CASCADE".format("temp_table"))
            conn.commit()
        except Exception as e:
            print("Error dropping temporary table:", e)
            conn.rollback()  # rollback the transaction if an error occurs
            return 1
    return 0


def update_location(cur, conn, engine):
    query = "$select=locationId,name,locationNumber&{}"
    url = "{}/Location?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        return
    df = df.rename(
        columns={
            "locationId": "locationid",
            "locationNumber": "locationnumber",
        }
    )
    # table_name = "location"
    # temp_table_name = "temp_table"
    try:
        df.to_sql("temp_table", engine, if_exists="replace", index=False)
        upsert_query = sql.SQL(
            """
       INSERT INTO {table} (locationid, name, locationnumber)
       SELECT t.locationid, t.name, t.locationnumber
        FROM {temp_table} AS t
        ON CONFLICT (locationid) DO UPDATE
        SET name = EXCLUDED.name,
            locationid = EXCLUDED.locationid,
            locationnumber = EXCLUDED.locationnumber
       """
        ).format(
            table=sql.Identifier("location"), temp_table=sql.Identifier("temp_table")
        )
        cur.execute(upsert_query)
        conn.commit()
    except UniqueViolation:
        print("Error upserting data due to unique contraint violation.")
        return 1
    except Exception as e:
        print("Error writing to database:", e)
        return 1
    finally:
        try:
            # drop temporary table
            cur.execute("DROP TABLE IF EXISTS {} CASCADE".format("temp_table"))
            conn.commit()
        except Exception as e:
            print("Error dropping temporary table:", e)
            conn.rollback()  # rollback the transaction if an error occurs
            return 1
    return 0


def update_company(cur, conn, engine):
    query = "$select=companyId,name&{}"
    url = "{}/Company?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        return
    df = df.rename(
        columns={
            "companyId": "companyid",
        }
    )
    # table_name = "company"
    # temp_table_name = "temp_table"
    try:
        df.to_sql("temp_table", engine, if_exists="replace", index=False)
        upsert_query = sql.SQL(
            """
       INSERT INTO {table} (companyid, name)
       SELECT t.companyid, t.name
        FROM {temp_table} AS t
        ON CONFLICT (companyid) DO UPDATE
        SET name = EXCLUDED.name
       """
        ).format(
            table=sql.Identifier("company"), temp_table=sql.Identifier("temp_table")
        )
        cur.execute(upsert_query)
        conn.commit()
    except UniqueViolation:
        print("Error upserting data due to unique contraint violation.")
        return 1
    except Exception as e:
        print("Error writing to database:", e)
        return 1
    finally:
        try:
            # drop temporary table
            cur.execute("DROP TABLE IF EXISTS {} CASCADE".format("temp_table"))
            conn.commit()
        except Exception as e:
            print("Error dropping temporary table:", e)
            conn.rollback()  # rollback the transaction if an error occurs
            return 1
    return 0


def update_item(cur, conn, engine):
    query = "$select=itemId,name,category1,category2,category3&{}"
    url = "{}/Item?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        return
    df = df.rename(
        columns={
            "itemId": "itemid",
        }
    )
    # table_name = "item"
    # temp_table_name = "temp_table"
    try:
        df.to_sql("temp_table", engine, if_exists="replace", index=False)
        upsert_query = sql.SQL(
            """
       INSERT INTO {table} (itemid, name, category1, category2, category3)
       SELECT t.itemid, t.name, t.category1, t.category2, t.category3
        FROM {temp_table} AS t
        ON CONFLICT (itemid) DO UPDATE
        SET name = EXCLUDED.name,
            itemid = EXCLUDED.itemid,
            category1 = EXCLUDED.category1,
            category2 = EXCLUDED.category2,
            category3 = EXCLUDED.category3
       """
        ).format(table=sql.Identifier("item"), temp_table=sql.Identifier("temp_table"))
        cur.execute(upsert_query)
        conn.commit()
    except UniqueViolation:
        print("Error upserting data due to unique contraint violation.")
        return 1
    except Exception as e:
        print("Error writing to database:", e)
        return 1
    finally:
        try:
            # drop temporary table
            cur.execute("DROP TABLE IF EXISTS {} CASCADE".format("temp_table"))
            conn.commit()
        except Exception as e:
            print("Error dropping temporary table:", e)
            conn.rollback()  # rollback the transaction if an error occurs
            return 1
    return 0


if __name__ == "__main__":
    with DatabaseConnection() as db:
        update_glaccount(db.cur, db.conn, db.engine)
        update_jobtitle(db.cur, db.conn, db.engine)
        update_location(db.cur, db.conn, db.engine)
        update_company(db.cur, db.conn, db.engine)
        update_item(db.cur, db.conn, db.engine)
