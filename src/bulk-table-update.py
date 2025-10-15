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


def make_HTTP_request(url):
    all_records = []
    while True:
        if not url:
            break
        try:
            r = requests.get(
                url, auth=(Config.SRVC_USER, Config.SRVC_PSWRD), timeout=30
            )
            r.raise_for_status()
        except requests.exceptions.Timeout:
            print(f"Timeout while trying to reach {url}")
            # handle timeout case, maybe retry or skip
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            # handle other errors
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
    url_filter = "$filter=date ge {}T00:00:00Z and date le {}T00:00:00Z".format(
        start, end
    )
    query = "$select=transactionId,locationId,transactionNumber,companyId,date,type&{}".format(
        url_filter
    )
    url = "{}/Transaction?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)

    if df.empty:
        logging.info("No data returned for the given date range.")
        return
    df["transactionNumber"] = df["transactionNumber"].astype(str)
    split_columns = df["transactionNumber"].str.split(" - ", expand=True)
    df["transactionNumber"] = split_columns.apply(
        lambda x: x[1] if len(x) > 1 else x[0], axis=1
    )
    # df["transactionNumber"] = split_columns[1].where(
    #     split_columns.shape[1] > 1, split_columns[0]
    # )
    df = df.rename(
        columns={
            "transactionId": "transactionid",
            "locationId": "locationid",
            "transactionNumber": "template",
            "companyId": "companyid",
        }
    )

    try:
        transaction_ids = df["transactionid"].unique().tolist()
        key_column = "transactionid"
        upload_to_database(df, "transaction", transaction_ids, key_column, cur, conn)
    except Exception as e:
        print(f"Failed to upload data to the database: {e}")
        raise
    return 0


def update_transaction_detail(start, end, cur, conn, engine):
    # get transactionid from transaction table and update transaction_detail table with transactionid
    url_filter = "$filter=date ge {}T00:00:00Z and date le {}T00:00:00Z".format(
        start, end
    )
    query = "$select=transactionId&{}".format(url_filter)
    url = "{}/Transaction?{}".format(Config.SRVC_ROOT, query)
    df = make_HTTP_request(url)
    if df.empty:
        logging.info("No data returned for the given date range.")
        return

    transid_list = df["transactionId"].tolist()
    transid_list = list(set(transid_list))
    # split transid_list into chunks of 10
    dfs = []
    for tl in tqdm(transid_list, desc="Fetching Transaction Details", unit="txn"):
        url_filter = "$filter=transactionId eq {}".format(tl)
        query = "$select=transactionId,locationId,glAccountId,itemId,credit,debit,amount,quantity,previousCountTotal,adjustment,unitOfMeasureName&{}".format(
            url_filter
        )
        url = "{}/TransactionDetail?{}".format(Config.SRVC_ROOT, query)
        df = make_HTTP_request(url)
        if not df.empty:
            dfs.append(df)
    if not dfs:
        return
    df = pd.concat(dfs, ignore_index=True)

    if df.empty:
        logging.info("No data returned for the given date range.")
        return

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
    # append a new column called date with values set to NULL
    df["date"] = None
    # query the transaction table for the date and transactionid
    query = "SELECT date, transactionid FROM transaction WHERE transactionid = ANY(%s)"
    df2 = pd.read_sql(query, engine, params=(transid_list,))
    # query = """ SELECT date, transactionid FROM transaction
    #             WHERE date >= '{}' AND date < '{}'
    #             ORDER BY date""".format(
    #     start, end
    # )
    # df2 = pd.read_sql(query, engine)
    # where transaction id match add the date to the transaction detail table
    df = pd.merge(df, df2, on=["transactionid"])
    # drop date_x and rename date_y to date
    df.drop("date_x", axis=1, inplace=True)
    df.rename(columns={"date_y": "date"}, inplace=True)
    # convert date to datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].isnull().any():
        raise ValueError("Invalid date format encountered in the data.")

    try:
        if transid_list:
            cur.execute(
                'DELETE FROM "transaction_detail" WHERE transactionid = ANY(%s)',
                (transid_list,),
            )
            conn.commit()
            print(f"Deleted {cur.rowcount} rows from table: transaction_detail")
    except Exception as e:
        logging.error("Error deleting data: %s", e)
        conn.rollback()
        return 1

    try:
        columns = list(df.columns)
        values = [tuple(x) for x in df.to_numpy()]
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier("transaction_detail"),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
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
    df = make_HTTP_request(url)
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


def update_sales_detail(start, end, cur, conn, engine):
    url_filter = "$filter=date ge {}T00:00:00Z and date le {}T00:00:00Z".format(
        start, end
    )
    url = "{}/SalesDetail?{}".format(Config.SRVC_ROOT, url_filter)
    df = make_HTTP_request(url)

    if df.empty:
        logging.info("No data returned for the given date range.")
        return
    df["menuitem"] = df["menuitem"].str.split(" - ", expand=True)[1]
    # drop unwanted columns
    df.drop(
        columns=[
            "customerPOSText",
            "void",
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
        target=sql.Identifier("sales_detail"),
        temp=sql.Identifier("temp_table"),
    )

    cur.execute(delete_query)
    conn.commit()
    logging.info(
        "Deleted old sales_employee records with outdated dailysalessummaryid."
    )

    try:
        salesdetail_ids = df["salesdetailid"].unique().tolist()
        key_column = "salesdetailid"
        upload_to_database(df, "sales_detail", salesdetail_ids, key_column, cur, conn)
    except Exception as e:
        print(f"Failed to upload data to the database: {e}")
        raise
    return 0


def update_sales_employee(start, end, cur, conn, engine):
    url_filter = "$filter=date ge {}T00:00:00Z and date le {}T00:00:00Z".format(
        start, end
    )
    url = "{}/SalesEmployee?{}".format(Config.SRVC_ROOT, url_filter)
    df = make_HTTP_request(url)

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
    df = make_HTTP_request(url)

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


def main(start_date, end_date, step, cur, conn, engine):
    update_function = [
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
