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
server_adress = "192.168.1.128"
db_name = "default"
table_name = "minutes"

con, _ = tapi.connect(token)
df_etf = tapi.get_instruments(con=con, instrument="Etf")
df_stock = tapi.get_instruments(con=con, instrument="Stock")
df_bond = tapi.get_instruments(con=con, instrument="Bond")


# 3. Testing etf querrying
endTime = datetime.now()
startTime = endTime - timedelta(days=10)
list_of_securities = []
df_data_etf = tapi.get_detailed_data(
    con=con, data=df_etf, _from=startTime, to=endTime, days_span=10
)
list_of_securities.append(df_data_etf)
df_data_bonds = tapi.get_detailed_data(
    con=con, data=df_bond, _from=startTime, to=endTime, days_span=10
)
list_of_securities.append(df_data_bonds)
df_data_stock = tapi.get_detailed_data(
    con=con, data=df_stock, _from=startTime, to=endTime, days_span=10
)
list_of_securities.append(df_data_stock)


timer_string = utilities_timers.format_timer_string(time.time() - start)
logger.info(f"Script runs for {timer_string}")
