# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 15:56:08 2020

@author: SParkhonyuk
"""
# this file will require the following package installation:
# conda install -c conda-forge clickhouse-driver
import os
from dotenv import load_dotenv, find_dotenv
from clickhouse_driver import Client
import pandas as pd
import logging
import logging.config
from datetime import datetime, timedelta
import time

# in-house
import utilities_timers

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)
usr = os.environ.get("CLICKHOUSE_USER")
pwd = os.environ.get("CLICKHOUSE_PWD")
# -----------------------------------------------------------------------------
def connect(instance_name="localhost"):
    """
    

    Parameters
    ----------
    instance_name : string
        ip adress of Clickhouse instance(i.e. 192.168.1.128). localhost by default.
        Username and password taken from .env file

    Returns
    -------
    Connector. Client object of clickhouse_driver.client module
    ping (list of tuples): responce from server

    """
    logger = logging.getLogger("Clickhouse::" + connect.__name__)

    logger.info(f"connection to {instance_name } is in progress ...")
    client = Client(instance_name, user=usr, password=pwd)

    try:
        ping = client.execute("SELECT 1")
    # except InterfaceError: # not tested.
    #    print("InterfaceError error is catched.")
    except Exception as exc:
        # shall you see such kind of error:
        # InterfaceError: (pyodbc.InterfaceError) ('IM002', '[IM002] [Microsoft]
        # [ODBC Driver Manager] Data source name not found and
        # no default driver specified (0) (SQLDriverConnect)')
        # (Background on this error at: http://sqlalche.me/e/rvf5)
        #
        # then you need to download and install:
        # Microsoft ODBC Driver 17 for SQL Server
        #
        logger.error(f"generated an exception: {exc}")
        client = None
        ping = str(exc)
    else:
        logger.info("connected to database.")

    return client, ping


# -----------------------------------------------------------------------------
def close_connection(client):
    """
    
    Parameters
    ----------
    client : clickhouse connector. Client object of clickhouse_driver.client module
    Check if connection exists. If yes, closes it.

    Returns
    -------
    None.

    """

    if len(client.execute("SELECT 1")) > 0:
        client.disconnect()


# -----------------------------------------------------------------------------
def get_databases(client):
    """
    Parameters
    ----------
    client : clickhouse connector. Client object of clickhouse_driver.client module

    Returns
    -------
    df: Pandas dataframe that contains names of the databases.
    """
    result = client.execute("SHOW DATABASES")
    df = pd.DataFrame(result)
    return df


# -----------------------------------------------------------------------------
def get_tables_in_databases(client, database="default"):
    """
    

    Parameters
    ----------
    client : clickhouse connector. Client object of clickhouse_driver.client module
    database: (str). Name of database to connect. Default name is 'default'

    Returns
    -------
    df: Pandas dataframe that contains names of tables that exist in database.

    """
    result = client.execute(f"SHOW TABLES FROM {database}")
    df = pd.DataFrame(result)
    return df


# -----------------------------------------------------------------------------
def get_column_names_in_table(client, table_name):
    """Get names of columns of given table stored on Clickhouse server.

    Args:
        client : clickhouse connector. Client object of clickhouse_driver.client module
    
        table_name (string) : table name in SQL database. If not exists will crush.

    Returns:
        col_names (list) : list of column names
        col_dtypes (list) : list of column data types
    """

    # to get the columns. Replace 'name' by * to get 34 parameters of the table.
    # https://stackoverflow.com/questions/1054984/how-can-i-get-column-names-from-a-table-in-sql-server
    result = client.execute(f"DESCRIBE TABLE {table_name}")
    df = pd.DataFrame(result)
    col_names = df[0].to_list()
    col_dtypes = df[1].to_list()

    return col_names, col_dtypes


# -----------------------------------------------------------------------------
def get_SQL_table(connection, table_name):
    """Get data from given table stored on SQL server.
    Warning: Can take extreme amount of memory if you will querry whole minutes table!

    Args:
        client : clickhouse connector. Client object of clickhouse_driver.client module
        table_name (string) : table name in Clickhouse database. If not exists will crush.

    Returns:
        df (Pandas.DataFrame) : table with data
    """

    start = time.time()
    logger = logging.getLogger("Clickhouse::" + get_SQL_table.__name__)
    logger.info(f"read table {table_name} from Clickhouse")

    result, columns = connection.execute(
        f"SELECT * FROM {table_name}", with_column_types=True
    )
    df = pd.DataFrame(result, columns=[tuple[0] for tuple in columns])

    timer_string = utilities_timers.format_timer_string(time.time() - start)
    logger.info(timer_string)

    return df


# -------------------------------
##-------------------------------------------------------------------------------------------------
def query_data_by_time(
    channels_list,
    startTime=None,
    endTime=None,
    days_span=90,
    server_ip="localhost",
    table_name="minutes",
    instrument_type="Etf",
    data_freq="min",
):
    """
    Request data from SQL DataBase in time range [startTime, endTime].

    Args:
        channels_list (list) : list of channels to be quired from DataBase
        startTime (datetime) : data will be quired AFTER this time moment
            If not set: 1 year period from endTime.
        endTime (datetime) : data will be quired BEFORE this time moment
            If not set: datetime.now() is used.
        days_span (int) : number of days before the endTime, if StartTime is not set.
        server_ip : string
        ip adress of Clickhouse instance(i.e. 192.168.1.128). localhost by default.
        Username and password taken from .env file
        table_name (string) : table name in SQL database.minutes by default

    Returns
        (pd.DataFrame) : Table with requested channels in a given time range.
    """
    start = time.time()
    logger = logging.getLogger("Clickhouse::" + query_data_by_time.__name__)
    logger.info("data query in progress ...")

    if channels_list == None:
        channels_list = ["*"]
    if len(channels_list) == 0:
        channels_list = ["*"]
    if endTime == None:
        endTime = datetime.now()

    if startTime == None:
        startTime = endTime - timedelta(days=days_span)
    # Check-up on proper instrument type
    if instrument_type not in ["Etf", "Bond", "Stock"]:
        logger.error("Unsupported instrument type. Only Etf, Bond, Stock are supported")
        logger.error("Does not querry anything")
        return None
    if data_freq not in ["min", "day", "week"]:
        logger.error("Unsupported data frequency. Only min, day, week are supported")
        logger.error("Does not querry anything")
        return None

    con, _ = connect(server_ip)
    channel_string = (" ,").join(channels_list)
    # converting values to strings to get clickhouse-compatible time format
    startTime = startTime.strftime("%Y-%m-%d %H:%M:%S")
    endTime = endTime.strftime("%Y-%m-%d %H:%M:%S")
    if data_freq == "min":

        msg1 = f"select {channel_string} from {table_name} "
        msg2 = f"where time BETWEEN '{startTime}' AND '{endTime}' "
        msg3 = f"AND type='{instrument_type}'"
        query = msg1 + msg2 + msg3
    elif data_freq == "day":
        # startTime = startTime.strftime("%Y-%m-%d")
        # endTime = endTime.strftime("%Y-%m-%d")
        logger.info(f"startTime is {startTime}")
        logger.info(f"endTIme is {endTime}")
        msg1 = f"select uniq(time), count(), ticker, type,currency,name, day, argMin(o, time) as o,max(h) as h, min(l) as l, argMax(c, time) as c, sum(v) as v "
        msg2 = f"from minutes "
        # msg3="where day BETWEEN toDate('{startTime}') AND toDate('{endTime}') AND type='{instrument_type}' "
        msg3 = f"where time BETWEEN '{startTime}' AND '{endTime}' AND type='{instrument_type}' "
        msg4 = "GROUP BY day, ticker, type, currency, name ORDER BY day desc"
        query = msg1 + msg2 + msg3 + msg4
    elif data_freq == "week":
        # startTime = startTime.strftime("%Y-%m-%d")
        # endTime = endTime.strftime("%Y-%m-%d")
        msg1 = "SELECT uniq(time), count(),ticker,currency, name, toMonday(day) as monday, argMin(o, time) as o, max(h) as h, min(l) as l, argMax(c, time) as c, sum(v) as v "
        msg2 = f"from minutes "
        msg3 = f"where time BETWEEN '{startTime}' AND '{endTime}' AND type='{instrument_type}' "
        msg4 = "GROUP BY monday, ticker, type, currency, name ORDER BY monday desc"
        query = msg1 + msg2 + msg3 + msg4

    logger.info(f"query string: {query}")
    # logger.info(f"query params: {params}")
    result, columns = con.execute(query, with_column_types=True)
    df = pd.DataFrame(result, columns=[tuple[0] for tuple in columns])

    close_connection(con)

    timer_string = utilities_timers.format_timer_string(time.time() - start)
    logger.info(timer_string)
    logger.info(f"data query complete. Dataframe has shape : {df.shape}.")
    logger.info("--------------------------")

    return df


##-------------------------------------------------------------------------------------------------
def append_df_to_SQL_table(
    df=None, table_name="minutes", server_ip="localhost", is_tmp_table_to_delete=True,
):
    """write dataframe to SQL server. Only values with unique columns will be append.

    Args:
        df (Pandas.DataFrame) : data to be written to SQL
        table_name (string) : table name in Clickhouse database."minutes" by default.
        server_ip (string) : Server IP. 'localhost' by default
        is_tmp_table_to_delete (bool) :
            True (default) to delete temporary table,
            which was used to keep new data when merging with the existing table.
            Set False if you want to keep tmp table for any reason (e.g. merge this table to multiple tables).

    Returns:
        nothing

    Raises:
        nothing

    """
    start = time.time()

    logger = logging.getLogger("Clickhouse::" + append_df_to_SQL_table.__name__)

    ##---------------------------------------------------------------------------------------------
    n_rows, n_cols = df.shape

    logger.info(f"write (row x col) : ({n_rows} x {n_cols})")
    expected_min = 1 * n_rows * n_cols / 130000 / 34  # experimental formula for ru0138
    logger.info(f"expected time ~{int(expected_min)} minutes")

    if n_rows == 0:
        logger.info(f"DataFrame has 0 rows. Target table will not be modified. Exit.")
        return

    logger.info(f"list of columns in df: {df.columns}.")

    if table_name[0].isdigit():
        table_name = "_" + table_name
        logger.warning(f"table name started from digit. Rename as {table_name}.")

    con, ping = connect(server_ip)
    logger.info(f"data uploading to a temp table  ...")
    # dropping table tmp
    r1 = con.execute("DROP TABLE IF EXISTS tmp")
    # creating new table
    r2 = con.execute(
        "CREATE TABLE tmp ("
        "figi String, "
        "interval String, o Float64, "
        "c Float64, h Float64, "
        "l Float64, v Int64, "
        "time DateTime, ticker String, "
        "isin String, min_price_increment Float64,"
        "lot Int64, currency String,"
        "name String, type String) ENGINE = Log "
    )
    con.execute("INSERT INTO tmp VALUES", [tuple(x) for x in df.values])

    inserted_rows_count = con.execute("SELECT count(*) FROM tmp")[0][0]
    logger.info(
        f"Inserted {inserted_rows_count} rows to temporary table. Moving to 'minutes' table"
    )
    initial_rows_count = con.execute("SELECT count(*) FROM minutes")[0][0]
    logger.info(f"initially minutes table has {initial_rows_count} rows")
    r3 = con.execute(
        "INSERT INTO minutes "
        "SELECT DISTINCT "
        "toDate(time) AS day,"
        "figi, "
        "interval, o, "
        "c , h, "
        "l , v, "
        "time, ticker, "
        "isin, min_price_increment,"
        "lot, currency,"
        "name, type FROM tmp WHERE (ticker, time) NOT IN (SELECT (ticker, time) FROM minutes)"
    )

    logger.info(f"merge complete")
    new_rows_count = con.execute("SELECT count(*) FROM minutes")[0][0]
    inserted_row_count = new_rows_count - initial_rows_count
    logger.info(f"Inserted {inserted_row_count} unique rows to minutes table.")
    logger.info(f"now minutes table has {new_rows_count} rows")
    if is_tmp_table_to_delete:
        r1 = con.execute("DROP TABLE IF EXISTS tmp")

    timer_string = utilities_timers.format_timer_string(time.time() - start)
    logger.info(timer_string)

    logger.info("new data written to table 'minutes'")


##-----------------------------------------------------------------------------
##-------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    logging.config.fileConfig(fname="logger.conf", disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    logger.info("MySQLHelper main")

    # testing the functions.
    # 1. Connection to database:
    dbname = "192.168.1.128"
    testcon, _ = connect(dbname)
    logger.info(f"test connect: {testcon}")
    logger.info(
        f"expected answer: <clickhouse_driver.client.Client object at 0x000002CA21EEA148>"
    )
    close_connection(testcon)

    # 2.testing the data querying.
    # should return df with shape (3848, 17)
    server_adress = "192.168.1.128"
    db_name = "default"
    table_name = "minutes"
    con, ping = connect(server_adress)
    available_cols, available_cols_dtypes = get_column_names_in_table(
        client=con, table_name=table_name
    )
    cols = [
        "day",
        "figi",
        "interval",
        "o",
        "c",
        "h",
        "l",
        "v",
        "time",
        "ticker",
        "isin",
        "min_price_increment",
        "lot",
        "currency",
        "name",
        "type",
    ]

    startTime = datetime(2020, 1, 10, 11, 19, 9)
    frequency = ["min", "day", "week"]
    for freq in frequency:
        logger.info(f"querrying with {freq} frequency")
        df = query_data_by_time(
            channels_list=[], days_span=7, server_ip="192.168.1.128", data_freq=freq
        )
        logger.info(f"query returns table (rows, columns)={df.shape}")

    close_connection(con)

    ##---------------------------------------------------------------------------------------------
    is_test_read_SQL_table = False
    if is_test_read_SQL_table:
        server_adress = "192.168.1.128"
        db_name = "default"
        table_name = "minutes"
        con, ping = connect(server_adress)
        logger.info("Testing function get_SQL_table")
        df = get_SQL_table(connection=con, table_name=table_name)
        close_connection(con)

    ##---------------------------------------------------------------------------------------------
    is_test_append_table = False
    if is_test_append_table:
        server_adress = "192.168.1.128"
        db_name = "default"
        table_name = "minutes"
        con, ping = connect(server_adress)
        df.drop("day", axis=1, inplace=True)
        logger.info("Testing function append_df_to_SQL_table")
        append_df_to_SQL_table(
            df=df,
            table_name=table_name,
            server_ip=server_adress,
            is_tmp_table_to_delete=True,
        )

        close_connection(con)
