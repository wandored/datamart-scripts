import argparse
import json
import logging
import time
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
from psycopg2 import IntegrityError, sql
from psycopg2.extras import execute_values
from tqdm import tqdm

from db_utils.config import Config
from db_utils.dbconnect import DatabaseConnection


def fetch_calendar_dates(conn, cur, **kwargs):
    try:
        # Validate required parameters
        if "year" not in kwargs:
            raise ValueError("The 'year' parameter is required.")

        # Build query and parameters dynamically
        query = 'SELECT * FROM "calendar" WHERE year = %s'
        params = [kwargs["year"]]

        if "week" in kwargs:
            query += " AND period = %s AND week = %s LIMIT 1"
            params.extend([kwargs["period"], kwargs["week"]])
        elif "period" in kwargs:
            query += " AND period = %s LIMIT 1"
            params.extend([kwargs["period"]])
        else:
            query += " LIMIT 1"

        # Execute query
        cur.execute(query, tuple(params))
        data = cur.fetchall()
        conn.commit()

        # Extract and return dates
        if "week" in kwargs:
            print(data[0][7], data[0][8])
            return data[0][7], data[0][8]
        elif "period" in kwargs:
            print(data[0][9], data[0][10])
            return data[0][9], data[0][10]
        else:
            print(data[0][13], data[0][14])
            return data[0][13], data[0][14]

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Database operation failed: {e}")


def make_http_request(url, max_retries=3, timeout=60):
    all_records = []
    while url:
        for attempt in range(max_retries):
            try:
                r = requests.get(
                    url, auth=(Config.SRVC_USER, Config.SRVC_PSWRD), timeout=timeout
                )
                r.raise_for_status()
                break
            except requests.exceptions.Timeout:
                logging.warning(
                    f"Timeout while trying to reach {url}, attempt {attempt + 1}/{max_retries}"
                )
                time.sleep(2**attempt)
            except requests.exceptions.RequestException as e:
                logging.error(
                    f"Request failed: {e}, attempt {attempt + 1}/{max_retries}"
                )
                time.sleep(2**attempt)
        else:
            logging.error(f"Failed to fetch {url} after {max_retries} attempts.")
            break

        json_data = r.json()
        all_records.extend(json_data.get("value", []))
        url = json_data.get("@odata.nextLink")

    if not all_records:
        return pd.DataFrame()
    return pd.read_json(StringIO(json.dumps(all_records)))


def upload_to_database(df, table_name, keys, key_column, cur, conn):
    try:
        delete_query = sql.SQL("DELETE FROM {} WHERE {} = ANY(%s)").format(
            sql.Identifier(table_name), sql.Identifier(key_column)
        )
        cur.execute(delete_query, (keys,))
        conn.commit()
        print(f"Deleted {cur.rowcount} rows from {table_name}")
    except Exception as e:
        logging.error("Error deleting data: %s", e)
        conn.rollback()
        return 1

    try:
        # Create the INSERT query dynamically
        columns = list(df.columns)
        values = [tuple(x) for x in df.to_numpy()]
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name), sql.SQL(", ").join(map(sql.Identifier, columns))
        )
        execute_values(cur, insert_query, values)
        conn.commit()
    except IntegrityError as e:
        logging.error("Error writing to database: %s", e)
        conn.rollback()
        return 1
    except Exception as e:
        logging.error("Error writing to database: %s", e)
        conn.rollback()
        return 1

    return 0


def update_transaction(start, end, cur, conn, engine):
    url = (
        f"{Config.SRVC_ROOT}/Transaction"
        f"?$select=transactionId,locationId,transactionNumber,companyId,date,type"
        f"&$filter=date ge {start}T00:00:00Z and date le {end}T00:00:00Z"
    )
    df = make_http_request(url)

    if df.empty:
        logging.info("No data returned for the given date range.")
        return

    df["transactionNumber"] = (
        df["transactionNumber"].astype(str).str.split(" - ").str[-1]
    )

    df = df.rename(
        columns={
            "transactionId": "transactionid",
            "locationId": "locationid",
            "transactionNumber": "template",
            "companyId": "companyid",
        }
    )

    # Ensure datetime type
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].isnull().any():
        raise ValueError("Invalid date format in transaction data")

    try:
        cur.execute("BEGIN;")

        # Create temp staging table
        cur.execute("""
            CREATE TEMP TABLE temp_transaction (
                transactionid text,
                locationid text,
                template text,
                companyid text,
                date timestamptz,
                type text
            ) ON COMMIT DROP;
        """)

        # Bulk load into temp table
        values = [tuple(x) for x in df.to_numpy()]
        insert_temp = """
            INSERT INTO temp_transaction (
                transactionid, locationid, template, companyid, date, type
            ) VALUES %s
        """
        execute_values(cur, insert_temp, values)

        # Upsert into target table
        upsert_query = """
            INSERT INTO transaction (
                transactionid, locationid, template, companyid, date, type
            )
            SELECT
                t.transactionid,
                t.locationid,
                t.template,
                t.companyid,
                t.date,
                t.type
            FROM temp_transaction t
            ON CONFLICT (transactionid) DO UPDATE
            SET
                locationid = EXCLUDED.locationid,
                template   = EXCLUDED.template,
                companyid  = EXCLUDED.companyid,
                date       = EXCLUDED.date,
                type       = EXCLUDED.type;
        """

        cur.execute(upsert_query)

        conn.commit()
        logging.info(f"Upserted {len(df)} transactions")

        return df["transactionid"].tolist()

    except Exception as e:
        conn.rollback()
        logging.error("Error in update_transaction", exc_info=e)
        return 1
    # try:
    #     transaction_ids = df["transactionid"].unique().tolist()
    #     upload_to_database(
    #         df, "transaction", transaction_ids, "transactionid", cur, conn
    #     )
    # except Exception as e:
    #     logging.error(f"Failed to upload data to the database: {e}")
    #     raise


def update_transaction_detail(start, end, cur, conn, engine):
    logging.info(f"Updating transaction_detail for {start} to {end}")

    # Step 1: get all transactionIds for the date range
    url = (
        f"{Config.SRVC_ROOT}/Transaction"
        f"?$select=transactionId"
        f"&$filter=date ge {start}T00:00:00Z and date le {end}T00:00:00Z"
    )

    df_ids = make_http_request(url)

    if df_ids.empty:
        logging.info("No transactions found for date range.")
        return 0

    transid_list = df_ids["transactionId"].dropna().unique().tolist()

    # Step 2: fetch all details
    dfs = []
    for tl in tqdm(transid_list, desc="Fetching Transaction Details", unit="txn"):
        url = (
            f"{Config.SRVC_ROOT}/TransactionDetail"
            f"?$select=transactionId,locationId,glAccountId,itemId,"
            f"credit,debit,amount,quantity,previousCountTotal,adjustment,unitOfMeasureName"
            f"&$filter=transactionId eq {tl}"
        )
        df = make_http_request(url)
        if not df.empty:
            dfs.append(df)

    if not dfs:
        logging.warning(
            "No transaction_detail data returned. Aborting to avoid data loss."
        )
        return 1

    df = pd.concat(dfs, ignore_index=True)

    # Step 3: normalize columns
    df = df.rename(
        columns={
            "transactionId": "transactionid",
            "locationId": "locationid",
            "glAccountId": "glaccountid",
            "itemId": "itemid",
            "previousCountTotal": "previouscounttotal",
            "unitOfMeasureName": "unitofmeasurename",
        }
    )

    # Step 4: attach date from transaction table
    query = """
        SELECT transactionid, date
        FROM transaction
        WHERE date >= %s AND date < %s
    """
    df_tx = pd.read_sql(query, engine, params=(start, end))

    df = df.merge(df_tx, on="transactionid", how="inner")

    if df["date"].isnull().any():
        raise ValueError(
            "Null dates after merge — indicates missing parent transactions"
        )

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    try:
        cur.execute("BEGIN;")

        # Step 5: create temp staging table
        cur.execute("""
            CREATE TEMP TABLE temp_transaction_detail (
                transactionid text,
                locationid text,
                glaccountid text,
                itemid text,
                credit double precision,
                debit double precision,
                amount double precision,
                quantity double precision,
                previouscounttotal double precision,
                adjustment double precision,
                unitofmeasurename text,
                date timestamptz
            ) ON COMMIT DROP;
        """)

        # Step 6: bulk insert into temp table
        values = [tuple(x) for x in df.to_numpy()]
        insert_temp = """
            INSERT INTO temp_transaction_detail (
                transactionid, locationid, glaccountid, itemid,
                credit, debit, amount, quantity,
                previouscounttotal, adjustment, unitofmeasurename, date
            ) VALUES %s
        """
        execute_values(cur, insert_temp, values)

        # Step 7: delete by date range (partition-pruned)
        cur.execute(
            """
            DELETE FROM transaction_detail
            WHERE date >= %s AND date < %s
        """,
            (start, end),
        )

        # Step 8: insert from staging
        cur.execute("""
            INSERT INTO transaction_detail (
                transactionid, locationid, glaccountid, itemid,
                credit, debit, amount, quantity,
                previouscounttotal, adjustment, unitofmeasurename, date
            )
            SELECT
                transactionid, locationid, glaccountid, itemid,
                credit, debit, amount, quantity,
                previouscounttotal, adjustment, unitofmeasurename, date
            FROM temp_transaction_detail;
        """)

        conn.commit()
        logging.info(
            f"Rebuilt transaction_detail for {start} to {end} ({len(df)} rows)"
        )

        return 0

    except Exception as e:
        conn.rollback()
        logging.error("Error in update_transaction_detail", exc_info=e)
        return 1


# def update_transaction_detail(start, end, cur, conn, engine):
#     # get transactionid from transaction table and update transaction_detail table with transactionid
#     logging.info(f"Updating transaction_detail for {start} to {end}")
#
#     url_filter = "$filter=date ge {}T00:00:00Z and date le {}T00:00:00Z".format(
#         start, end
#     )
#     query = "$select=transactionId&{}".format(url_filter)
#     url = "{}/Transaction?{}".format(Config.SRVC_ROOT, query)
#     df = make_http_request(url)
#     if df.empty:
#         logging.info("No data returned for the given date range.")
#         return
#
#     transid_list = df["transactionId"].tolist()
#     transid_list = list(set(transid_list))
#     # split transid_list into chunks of 10
#     dfs = []
#     for tl in tqdm(transid_list, desc="Fetching Transaction Details", unit="txn"):
#         url_filter = "$filter=transactionId eq {}".format(tl)
#         query = "$select=transactionId,locationId,glAccountId,itemId,credit,debit,amount,quantity,previousCountTotal,adjustment,unitOfMeasureName&{}".format(
#             url_filter
#         )
#         url = "{}/TransactionDetail?{}".format(Config.SRVC_ROOT, query)
#         df = make_http_request(url)
#         if not df.empty:
#             dfs.append(df)
#     if not dfs:
#         return
#     df = pd.concat(dfs, ignore_index=True)
#
#     if df.empty:
#         logging.info("No data returned for the given date range.")
#         return
#
#     df = df.rename(
#         columns={
#             "transactionId": "transactionid",
#             "locationId": "locationid",
#             "glAccountId": "glaccountid",
#             "itemId": "itemid",
#             "previousCountTotal": "previouscounttotal",
#             "unitOfMeasureName": "unitofmeasurename",
#         }
#     )
#     # append a new column called date with values set to NULL
#     df["date"] = None
#     # query the transaction table for the date and transactionid
#     query = "SELECT date, transactionid FROM transaction WHERE transactionid = ANY(%s)"
#     df2 = pd.read_sql(query, engine, params=(transid_list,))
#     # where transaction id match add the date to the transaction detail table
#     df = pd.merge(df, df2, on=["transactionid"])
#     # drop date_x and rename date_y to date
#     df.drop("date_x", axis=1, inplace=True)
#     df.rename(columns={"date_y": "date"}, inplace=True)
#     # convert date to datetime
#     df["date"] = pd.to_datetime(df["date"], errors="coerce")
#     if df["date"].isnull().any():
#         raise ValueError("Invalid date format encountered in the data.")
#
#     try:
#         if transid_list:
#             cur.execute(
#                 'DELETE FROM "transaction_detail" WHERE transactionid = ANY(%s)',
#                 (transid_list,),
#             )
#             conn.commit()
#             print(f"Deleted {cur.rowcount} rows from table: transaction_detail")
#     except Exception as e:
#         logging.error("Error deleting data: %s", e)
#         conn.rollback()
#         return 1
#
#     try:
#         columns = list(df.columns)
#         values = [tuple(x) for x in df.to_numpy()]
#         insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
#             sql.Identifier("transaction_detail"),
#             sql.SQL(", ").join(map(sql.Identifier, columns)),
#         )
#         execute_values(cur, insert_query, values)
#         conn.commit()
#     except IntegrityError as e:
#         logging.error("Error writing to database: %s", e)
#         conn.rollback()
#         return 1
#     except Exception as e:
#         logging.error("Error writing to database: %s", e)
#         conn.rollback()
#         return 1
#
#     return 0


def update_labor_detail(start, end, cur, conn, engine):
    # dateworked does not have time so time is 00:00:00
    url_filter = (
        "$filter=dateWorked ge {}T00:00:00Z and dateWorked lt {}T00:00:00Z".format(
            start, end
        )
    )
    query = "$select=dateWorked,hours,total,jobTitle_Id,location_ID,jobTitle,dailySalesSummaryId,laborId&{}".format(
        url_filter
    )
    url = "{}/LaborDetail?{}".format(Config.SRVC_ROOT, query)
    df = make_http_request(url)
    if df.empty:
        logging.info("No data returned for the given date range.")
        return

    df = df.rename(
        columns={
            "dailySalesSummaryId": "dailysalessummaryid",
            "dateWorked": "dateworked",
            "jobTitle_ID": "jobtitle_id",
            "location_ID": "location_id",
            "jobTitle": "jobtitle",
            "laborId": "laborid",
        }
    )
    # make dateworked a datetime object
    df["dateworked"] = pd.to_datetime(df["dateworked"], errors="coerce")

    df.to_sql("temp_table", engine, if_exists="replace", index=False)
    # Remove old records for the same location and date, but different dailysalessummaryid
    delete_query = sql.SQL(
        """
        DELETE FROM {target}
        USING {temp}
        WHERE {target}.location_id = {temp}.location_id
        AND {target}.dateworked = {temp}.dateworked
        AND {target}.dailysalessummaryid <> {temp}.dailysalessummaryid
        """
    ).format(
        target=sql.Identifier("labor_detail"),
        temp=sql.Identifier("temp_table"),
    )

    cur.execute(delete_query)
    conn.commit()
    logging.info(
        "Deleted old sales_employee records with outdated dailysalessummaryid."
    )
    try:
        id_list = df["laborid"].unique().tolist()
        for id in id_list:
            cur.execute('DELETE FROM "labor_detail" WHERE laborid = %s', (id,))
        conn.commit()
        print(f"Deleted {cur.rowcount} rows from table: labor_detail")
    except Exception as e:
        logging.error("Error deleting data: %s", e)
        conn.rollback()
        return 1

    try:
        columns = list(df.columns)
        values = [tuple(x) for x in df.to_numpy()]
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier("labor_detail"),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
        )
        execute_values(cur, insert_query, values)
        conn.commit()
    except IntegrityError:
        logging("Error writing to database: %s", e)
        conn.rollback()
        return 1
    except Exception as e:
        logging.error("Error writing to database: %s", e)
        conn.rollback()
        return 1

    return 0


# def update_sales_detail(start, end, cur, conn, engine):
#     url_filter = "$filter=date ge {}T00:00:00Z and date le {}T00:00:00Z".format(
#         start, end
#     )
#     url = "{}/SalesDetail?{}".format(Config.SRVC_ROOT, url_filter)
#     df = make_http_request(url)
#
#     if df.empty:
#         logging.info("No data returned for the given date range.")
#         return
#     df["menuitem"] = df["menuitem"].str.split(" - ", expand=True)[1]
#     # drop unwanted columns
#     df.drop(
#         columns=[
#             "customerPOSText",
#             "company",
#             "salesID",
#             "houseAccountTransaction",
#             "transactionDetailID",
#             "cateringEvent",
#             "createdOn",
#             "modifiedOn",
#             "menuItemId",
#             "createdBy",
#             "modifiedBy",
#         ],
#         inplace=True,
#     )
#
#     df = df.rename(
#         columns={
#             "salesdetailID": "salesdetailid",
#             "salesAccount": "salesaccount",
#             "dailySalesSummaryId": "dailysalessummaryid",
#         }
#     )
#     df["date"] = pd.to_datetime(df["date"], errors="coerce")
#     df.to_sql("temp_table", engine, if_exists="replace", index=False)
#     # Remove old records for the same location and date, but different dailysalessummaryid
#     # delete_query = sql.SQL(
#     #     """
#     #     DELETE FROM {target}
#     #     USING {temp}
#     #     WHERE {target}.location = {temp}.location
#     #     AND {target}.date = {temp}.date
#     #     AND {target}.dailysalessummaryid <> {temp}.dailysalessummaryid
#     #     """
#     # ).format(
#     #     target=sql.Identifier("sales_detail"),
#     #     temp=sql.Identifier("temp_table"),
#     # )
#     delete_query = sql.SQL("""
#         DELETE FROM {target} t
#         WHERE (t.date, t.location) IN (
#             SELECT DISTINCT date, location FROM {temp}
#         )
#         AND NOT EXISTS (
#             SELECT 1
#             FROM {temp} s
#             WHERE s.date = t.date
#               AND s.location = t.location
#               AND s.dailysalessummaryid = t.dailysalessummaryid
#         )
#     """).format(
#         target=sql.Identifier("sales_detail"),
#         temp=sql.Identifier("temp_table"),
#     )
#
#     cur.execute(delete_query)
#     conn.commit()
#     logging.info(
#         "Deleted old sales_employee records with outdated dailysalessummaryid."
#     )
#
#     try:
#         salesdetail_ids = df["salesdetailid"].unique().tolist()
#         key_column = "salesdetailid"
#         upload_to_database(df, "sales_detail", salesdetail_ids, key_column, cur, conn)
#     except Exception as e:
#         print(f"Failed to upload data to the database: {e}")
#         raise
#     return 0


def update_sales_detail(start, end, cur, conn, engine):
    url_filter = "$filter=date ge {}T00:00:00Z and date le {}T00:00:00Z".format(
        start, end
    )
    url = "{}/SalesDetail?{}".format(Config.SRVC_ROOT, url_filter)

    df = make_http_request(url)

    if df.empty:
        logging.info("No data returned for the given date range.")
        return 0

    # --- Transformations ---
    df["menuitem"] = df["menuitem"].str.split(" - ", expand=True)[1]

    df.drop(
        columns=[
            "customerPOSText",
            "company",
            "salesID",
            "houseAccountTransaction",
            "transactionDetailID",
            "cateringEvent",
            "createdOn",
            "modifiedOn",
            "menuItemId",
            "createdBy",
            "modifiedBy",
        ],
        inplace=True,
    )

    df = df.rename(
        columns={
            "salesdetailID": "salesdetailid",
            "salesAccount": "salesaccount",
            "dailySalesSummaryId": "dailysalessummaryid",
        }
    )
    dss_ids = df["dailysalessummaryid"].unique().tolist()
    print(f"{len(dss_ids)} DSS Id's on {df['date']}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # --- Load into temp table ---
    temp_table = "temp_sales_detail"

    df.to_sql(temp_table, engine, if_exists="replace", index=False)

    try:
        # Wrap everything in a single transaction
        with conn:
            with conn.cursor() as cur:
                # 1. Delete obsolete DSS versions (set-based)
                delete_query = sql.SQL("""
                    DELETE FROM sales_detail t
                    WHERE (t.date, t.location) IN (
                        SELECT DISTINCT date, location FROM {temp}
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM {temp} s
                        WHERE s.date = t.date
                          AND s.location = t.location
                          AND s.dailysalessummaryid = t.dailysalessummaryid
                    )
                """).format(temp=sql.Identifier(temp_table))

                cur.execute(delete_query)
                logging.info("Deleted obsolete DSS versions from sales_detail.")

                # 2. Upsert from temp table
                upsert_query = sql.SQL("""
                    INSERT INTO sales_detail (
                        salesdetailid,
                        date,
                        location,
                        dailysalessummaryid,
                        salesaccount,
                        menuitem,
                        quantity,
                        amount
                    )
                    SELECT
                        salesdetailid,
                        date,
                        location,
                        dailysalessummaryid,
                        salesaccount,
                        menuitem,
                        quantity,
                        amount
                    FROM {temp}
                    ON CONFLICT (salesdetailid, date)
                    DO UPDATE SET
                        location = EXCLUDED.location,
                        dailysalessummaryid = EXCLUDED.dailysalessummaryid,
                        salesaccount = EXCLUDED.salesaccount,
                        menuitem = EXCLUDED.menuitem,
                        quantity = EXCLUDED.quantity,
                        amount = EXCLUDED.amount
                """).format(temp=sql.Identifier(temp_table))

                cur.execute(upsert_query)
                logging.info("Upserted sales_detail records.")

    except Exception as e:
        logging.error(f"Failed to update sales_detail: {e}")
        raise

    return 0


def update_sales_employee(start, end, cur, conn, engine):
    url_filter = "$filter=date ge {}T00:00:00Z and date le {}T00:00:00Z".format(
        start, end
    )
    url = "{}/SalesEmployee?{}".format(Config.SRVC_ROOT, url_filter)
    df = make_http_request(url)

    if df.empty:
        logging.info("No data returned for the given date range.")
        return
    # drop unwanted columns
    df.drop(
        columns=[
            "receiptNumber",
            "checkNumber",
            "comment",
            "dayOfWeek",
            "taxAmount",
            "tipAmount",
            "totalAmount",
            "totalPayment",
            "void",
            "server",
            "createdOn",
            "modifiedOn",
            "serviceType",
            "createdBy",
            "modifiedBy",
        ],
        inplace=True,
    )
    df = df.rename(
        columns={
            "salesId": "salesid",
            "dayPart": "daypart",
            "netSales": "netsales",
            "numberofGuests": "numberofguests",
            "orderHour": "orderhour",
            "salesAmount": "salesamount",
            "grossSales": "grosssales",
            "dailySalesSummaryId": "dailysalessummaryid",
        }
    )
    # Ensure no nulls in important fields
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df.to_sql("temp_table", engine, if_exists="replace", index=False)
    # Remove old records for the same location and date, but different dailysalessummaryid
    delete_query = sql.SQL(
        """
        DELETE FROM {target}
        USING {temp}
        WHERE {target}.location = {temp}.location
        AND {target}.date = {temp}.date
        AND {target}.dailysalessummaryid <> {temp}.dailysalessummaryid
        """
    ).format(
        target=sql.Identifier("sales_employee"),
        temp=sql.Identifier("temp_table"),
    )

    cur.execute(delete_query)
    conn.commit()
    logging.info(
        "Deleted old sales_employee records with outdated dailysalessummaryid."
    )

    try:
        sales_ids = df["salesid"].unique().tolist()
        key_column = "salesid"
        upload_to_database(df, "sales_employee", sales_ids, key_column, cur, conn)
    except Exception as e:
        print(f"Failed to upload data to the database: {e}")
        raise
    return 0


def update_sales_payment(start, end, cur, conn, engine):
    url_filter = "$filter=date ge {}T00:00:00Z and date le {}T00:00:00Z".format(
        start, end
    )
    url = "{}/SalesPayment?{}".format(Config.SRVC_ROOT, url_filter)
    df = make_http_request(url)

    if df.empty:
        logging.info("No data returned for the given date range.")
        return

    # drop unwanted columns
    df.drop(
        columns=[
            "comment",
            "customerPOSText",
            "isException",
            "missingreceipt",
            "company",
            "salesID",
            "houseAccountTransaction",
            "transactionDetailID",
            "cateringEvent",
            "exclude",
            "createdOn",
            "modifiedOn",
            "paymentGroup",
            "createdBy",
            "modifiedBy",
        ],
        inplace=True,
    )

    df = df.rename(
        columns={
            "salespaymentId": "salespaymentid",
            "dailySalesSummaryId": "dailysalessummaryid",
        }
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df.to_sql("temp_table", engine, if_exists="replace", index=False)
    # Remove old records for the same location and date, but different dailysalessummaryid
    delete_query = sql.SQL(
        """
        DELETE FROM {target}
        USING {temp}
        WHERE {target}.location = {temp}.location
        AND {target}.date = {temp}.date
        AND {target}.dailysalessummaryid <> {temp}.dailysalessummaryid
        """
    ).format(
        target=sql.Identifier("sales_payment"),
        temp=sql.Identifier("temp_table"),
    )

    cur.execute(delete_query)
    conn.commit()
    logging.info(
        "Deleted old sales_employee records with outdated dailysalessummaryid."
    )

    try:
        salespayment_ids = df["salespaymentid"].unique().tolist()
        key_column = "salespaymentid"
        upload_to_database(df, "sales_payment", salespayment_ids, key_column, cur, conn)
    except Exception as e:
        print(f"Failed to upload data to the database: {e}")
        raise
    return 0


def get_dss_list(start, end, cur, conn, engine):
    url = (
        f"{Config.SRVC_ROOT}/salesPayment"
        f"?$filter=date ge {start}T00:00:00Z and date lt {end}T00:00:00Z"
    )
    df = make_http_request(url)
    # remove time from date
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[["date", "dailySalesSummaryId", "location", "salespaymentId"]]
    if df.empty:
        logging.info("No data returned for the given date range.")
        return
    df = df.drop_duplicates()
    print(df)
    return


def main(start_date, end_date, step, cur, conn, engine):

    update_function = [
        # get_dss_list,
        update_transaction,
        update_transaction_detail,
        update_labor_detail,
        update_sales_detail,
        update_sales_employee,
        update_sales_payment,
    ]

    for current_function in update_function:
        current_date = start_date
        start_time = time.time()

        total_time = time.time() - start_time

        while current_date < end_date:
            period = current_date + step
            current_function(current_date, period, cur, conn, engine)
            current_date = current_date + step
            total_time = time.time() - start_time
        print(
            f"Time elapsed to run {current_function.__name__}: {total_time:.2f} seconds\n"
        )
        print()

    return 0


if __name__ == "__main__":
    print(f"Script run time: {datetime.now()}")

    # Create and parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--year", help="Year to run the script")
    parser.add_argument("-p", "--period", help="Period to run the script")
    parser.add_argument("-w", "--week", help="Week to run the script")
    args = parser.parse_args()

    with DatabaseConnection() as db:
        # Determine the start and end dates based on arguments
        if args.year and args.period and args.week:
            start_date, end_date = fetch_calendar_dates(
                cur=db.cur,
                conn=db.conn,
                year=args.year,
                period=args.period,
                week=args.week,
            )
            step = timedelta(days=1)
        elif args.year and args.period:
            start_date, end_date = fetch_calendar_dates(
                cur=db.cur, conn=db.conn, year=args.year, period=args.period
            )
            step = timedelta(days=1)
        elif args.year:
            start_date, end_date = fetch_calendar_dates(
                cur=db.cur, conn=db.conn, year=args.year
            )
            step = timedelta(days=1)
        else:
            print("You must provide a year\n")
            parser.print_help()
            exit(1)

        main(start_date, end_date, step, db.cur, db.conn, db.engine)
