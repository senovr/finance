# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 12:47:00 2020

@author: SParkhonyuk
"""
# Tinkoff's API implementation in python pip install -i https://test.pypi.org/simple/ --extra-index-url=https://pypi.org/simple/ tinkoff-invest-openapi-client
from openapi_client import openapi
from datetime import datetime, timedelta
from pytz import timezone
import pandas as pd
import time
from dotenv import load_dotenv, find_dotenv
import os

import logging
import logging.config
from datetime import datetime, timedelta
from tqdm import tqdm

# in-house
import utilities_timers

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)
token = os.environ.get("APIKEY_SANDBOX")

client = openapi.sandbox_api_client(token)
client.sandbox.sandbox_register_post()
client.sandbox.sandbox_clear_post()
client.sandbox.sandbox_currencies_balance_post(
    sandbox_set_currency_balance_request={"currency": "USD", "balance": 1000}
)
#Creating local variable for API name
apiname="TinkoffAPI::"
timeformat="%Y-%m-%dT%H:%M:%S"
##-------------------------------------------------------------------------------------------------
def connect(token=None):
    """
    Function that creates connection to Tinkoff API

    Parameters
    ----------
    token : string
        API key for either sandbox or prod. Taken from .env file
        To get your key,
        log in to you account on tinkoff.ru
        Go to investment
        Go to settings
        Function "deals confirmation by code" should be turned off
        Issue a token for OpenAPI for sandbox and prod.
        If system asks you to authorize one more time-it's okay.
        Copy and paste token (to .env file for example)
        Token will be shown only once, but you can issue as many tokens as you want.

    Returns
    -------
    Connector. Client object of clickhouse_driver.client module
    ping (list of tuples): responce from server

    """
    logger = logging.getLogger(apiname + connect.__name__)

    logger.info(f"connection to TinkoffAPI is in progress ...")
    client = openapi.sandbox_api_client(token)

    try:
        ping = client.sandbox.sandbox_register_post()
        client.sandbox.sandbox_clear_post()
        client.sandbox.sandbox_currencies_balance_post(
            sandbox_set_currency_balance_request={"currency": "USD", "balance": 1000}
        )
    # except InterfaceError: # not tested.
    #    print("InterfaceError error is catched.")
    except Exception as exc:

        logger.error(f"generated an exception: {exc}")
        client = None
        ping = str(exc)
    else:
        logger.info("connected to API web service.")

    return client, ping


##-------------------------------------------------------------------------------------------------
def get_instruments(con=None, instrument=None):
    """
    Gets list of instruments (bonds, stocks, ETFs that traded on tinkoff-invest broker)

    Args:
        con(connector) : TinkoffAPI connector. Create one using connect() function
        instrument(str) : can be either stock, bond, etf. 
   
    Returns:
        df (Pandas dataframe) : list of instruments available for trade

    Raises:
        Error if not defined instrument or wrong instrument name is provided as input.
    """
    start = time.time()
    logger = logging.getLogger(apiname + connect.__name__)

    logger.info(f"retrieving available {instrument} is in progress ...")
    if instrument == "Stock":
        tickers = con.market.market_stocks_get_with_http_info()
    elif instrument == "Bond":
        tickers = con.market.market_bonds_get_with_http_info()
    elif instrument == "Etf":
        tickers = con.market.market_etfs_get_with_http_info()
    else:
        logger.error(
            f"Wrong instrument defined. Acceptable are Stock, Bond, Etf. You provided {instrument}"
        )
        return None
    list_of_tickers = []
    for item in tickers[0].payload.instruments:
        list_of_tickers.append(item.to_dict())
    df = pd.DataFrame.from_records(list_of_tickers)
    timer_string = utilities_timers.format_timer_string(time.time() - start)
    logger.info(timer_string)
    logger.info(f"retrieved dataframe with shape {df.shape} ")
    return df


##-------------------------------------------------------------------------------------------------
def detailed_history(
    con=None, figi="BBG00M0C8YM7", _from=None, to=None, interval="1min", days_span=10
):
    """
    

    Parameters
    ----------
    con(connector) : TinkoffAPI connector. Create one using connect() function
    figi : str, required
        FIGI identifier of instrument. The default is 'BBG00M0C8YM7'.
    _from : datetime, optional
        Date (beginning of the period for query). The default is None.
    to : datetime, optional
        Date (end of the period for query). The default is None.
    interval : str, required
        frequency of data to retrieve. The default is "1min".
    days_span : int, required
        How much day back to query from now. The default is 10 days.

    Returns
    -------
    df_merge : Pandas dataframe
        Contains candles data for selected FIGI for given time period.

    """
    start = time.time()
    logger = logging.getLogger(apiname + detailed_history.__name__)

    logger.info(f"retrieving available data is in progress ...")
    if to == None:
        to = datetime.now()
        endTime = to
    else:
        endTime = to
    if _from == None:
        _from = endTime - timedelta(days=days_span)
        startTime = _from
    else:
        startTime = _from

    startTime_list = []
    endTime_list = []

    startTime_list.append(startTime.strftime(timeformat) + "+07:00")
    endTime_list.append(endTime.strftime(timeformat) + "+07:00")

    if (endTime - startTime).days > 1:
        startTime_list = []
        endTime_list = []
        endTime = to
        for i in range((to - _from).days):
            # print(i)
            newStartTime = endTime - timedelta(days=1)
            startTime_list.append(newStartTime.strftime(timeformat) + "+07:00")
            endTime_list.append(endTime.strftime(timeformat) + "+07:00")
            endTime = newStartTime

    list_df = []
    for i in range(len(startTime_list)):
        tickers = con.market.market_candles_get_with_http_info(
            figi=figi, _from=startTime_list[i], to=endTime_list[i], interval="1min"
        )
        list_of_tickers = []
        for item in tickers[0].payload.candles:
            list_of_tickers.append(item.to_dict())
        df = pd.DataFrame.from_records(list_of_tickers)
        list_df.append(df)

    df_merge = pd.concat(list_df)
    timer_string = utilities_timers.format_timer_string(time.time() - start)
    logger.info(timer_string)
    logger.info(f"Shape of dataframe {df_merge.shape} ...")
    return df_merge


def get_detailed_data(con=None, data=None, _from=None, to=None, days_span=10):
    """
    Function that retrieves the data 

    Parameters
    ----------
    data : Dataframe that contains tickers and FIGIs of instrument. Get it using get_instruments()
        
    _from : datetime, optional
        Date (beginning of the period for query). The default is None.
    to : datetime, optional
        Date (end of the period for query). The default is None.
    interval : str, required
        frequency of data to retrieve. The default is "1min".@todo, not implemented
    days_span : int, required
        How much day back to query from now. The default is 10 days.

    Returns
    -------
    df_merge : TYPE
        DESCRIPTION.

    """
    start = time.time()
    logger = logging.getLogger("TinkoffAPI::" + get_detailed_data.__name__)

    logger.info(f"retrieving available data is in progress ...")
    if to == None:
        to = datetime.now()
        endTime = to
    else:
        endTime = to
    if _from == None:
        _from = endTime - timedelta(days=days_span)

    list_df = []
    for figi in tqdm(data.figi.unique()):
        logger.info(
            "retrieving figi: "
            + figi
            + " ticker: "
            + data[data.figi == figi].ticker.values
        )

        df = detailed_history(
            con=con, figi=figi, _from=_from, to=to, interval="1min", days_span=days_span
        )
        time.sleep(2)
        list_df.append(df)

    data.reset_index(inplace=True, drop=True)
    df_merge = pd.concat(list_df)
    df_merge = pd.merge(left=df_merge, right=data, left_on="figi", right_on="figi")
    timer_string = utilities_timers.format_timer_string(time.time() - start)
    logger.info(timer_string)
    logger.info(f"retrieved dataframe with shape {df_merge.shape} ")
    return df_merge


##-------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    logging.config.fileConfig(fname="logger.conf", disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    logger.info("tinkoffAPIHelper main")

    # testing the functions.
    # 1. Connection to API:
    
    testcon, _ = connect(token)
    logger.info(f"test connect: {testcon}")
    logger.info(
        f"expected answer: <openapi_client.openapi.SandboxOpenApi at 0x15b7294f4c8>"
    )
    # close_connection(testcon)

    # 2.testing the instrument  querying.
    logger.info(f"test get_instruments function")
    
    con, _ = connect(token)
    df_etf = get_instruments(con=con, instrument="Etf")
    df_stock = get_instruments(con=con, instrument="Stock")
    df_bond = get_instruments(con=con, instrument="Bond")

    # 3. Testing FIGI querrying
    logger.info(f"test detailed_history function")
    
    con, _ = connect(token)
    df = detailed_history(con, days_span=10)

    # 3. Testing etf querrying
    logger.info(f"test get_detailed_data function")
    
    con, _ = connect(token)
    df = get_detailed_data(con=con, data=df_etf, _from=None, to=None, days_span=10)

"""
    ##---------------------------------------------------------------------------------------------
    is_test_read_SQL_table = True
    if is_test_read_SQL_table:
        server_adress = "192.168.1.128"
        db_name = "default"
        table_name = "minutes"
        con, ping = connect(server_adress)
        logger.info('Testing function get_SQL_table')
        df = get_SQL_table(connection=con, table_name=table_name)
        close_connection(con)

    ##---------------------------------------------------------------------------------------------
    is_test_append_table = True
    if is_test_append_table:
        server_adress = "192.168.1.128"
        db_name = "default"
        table_name = "minutes"
        con, ping = connect(server_adress)
        df.drop("day",axis=1,inplace=True)
        logger.info('Testing function append_df_to_SQL_table')
        append_df_to_SQL_table(
            df=df, table_name=table_name, server_ip=server_adress, is_tmp_table_to_delete=True,
            )

        close_connection(con)
"""
