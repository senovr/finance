# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 14:49:17 2020

@author: SParkhonyuk
"""

import tinkoffAPIHelper as tapi
import pandas as pd
import ClickhouseHelper as chh
from dotenv import load_dotenv, find_dotenv
import os
from datetime import datetime, timedelta
import time
import logging
import logging.config
import utilities_timers

logging.config.fileConfig(fname="logger.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)
logger.info("Data downloader main")
start = time.time()
dotenv_path = find_dotenv()

load_dotenv(dotenv_path)
token = os.environ.get("APIKEY_SANDBOX")
usr = os.environ.get("CLICKHOUSE_USER")
pwd = os.environ.get("CLICKHOUSE_PWD")
# 2.testing the instrument  querying.
server_adress = os.environ.get("CLICKHOUSE_SERVER_ADRESS")
db_name = os.environ.get("CLICKHOUSE_DB_NAME")
table_name = os.environ.get("CLICKHOUSE_TABLE_NAME")

con, _ = tapi.connect(token)
df_etf = tapi.get_instruments(con=con, instrument="Etf")
df_stock = tapi.get_instruments(con=con, instrument="Stock")
df_bond = tapi.get_instruments(con=con, instrument="Bond")


# 3. Testing etf querrying
endTime = datetime.now()
startTime = endTime - timedelta(days=10)
list_of_securities = []

try:
    df_data_etf = tapi.get_detailed_data(
        con=con, data=df_etf, _from=startTime, to=endTime, days_span=90
    )
    list_of_securities.append(df_data_etf)
except Exception as error:
    logger.error("scrip failed during downloading ETF data")
    logger.error(f"exception catched: {error}")

try:
    df_data_bonds = tapi.get_detailed_data(
        con=con, data=df_bond, _from=startTime, to=endTime, days_span=10
    )
    list_of_securities.append(df_data_bonds)
except Exception as error:
    logger.error("scrip failed during downloading Bonds data")
    logger.error(f"exception catched: {error}")

try:
    df_data_stock = tapi.get_detailed_data(
        con=con, data=df_stock, _from=startTime, to=endTime, days_span=10
    )
    list_of_securities.append(df_data_stock)
except Exception as error:
    logger.error("scrip failed during downloading Stocks data")
    logger.error(f"exception catched: {error}")

df = pd.concat(list_of_securities)
logger.info("Creating connection to clickhouse")
con, ping = chh.connect(server_adress)

logger.info("Uploading data to clickhouse")
try:
    chh.append_df_to_SQL_table(
        df=df,
        table_name=table_name,
        server_ip=server_adress,
        is_tmp_table_to_delete=True,
    )
except Exception as error:
    logger.error("scrip failed during pushing data to clickhouse")
    logger.error(f"exception catched: {error}")

chh.close_connection(con)

timer_string = utilities_timers.format_timer_string(time.time() - start)
logger.info(f"Script runs for {timer_string}")
