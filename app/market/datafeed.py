import logging

import sys
import ccxt
import datetime
from tenacity import retry, retry_if_exception_type, stop_after_attempt
from exchange import ExchangeInterface
from sqlalchemy import select, and_
logger = logging.getLogger(__name__)

# Pull data from the exchange at a given interval and write to the database
#TODO:reconcile the difference between history and latest tickers
# How to control the trade given the live data feed
from market import database
from threading import Thread
engine = database.engine
conn = engine.connect()
import time as time_
import pandas as pd

def start_ticker(exchangeInterface,exchange, market_pair='BTC/USD',  interval='1h'):
    """Start a ticker/timer that notifies market watchers when to pull a new candle"""
    market_pair[interval] = Thread(
            target=__start_ticker, args=(
                exchangeInterface,exchange,),
                kwargs={market_pair:market_pair,interval:interval},name='start_ticker').start()
def get_latest_data(exchange,market_pair, interval,periods = 1):
    """

    :param exchange: name of the exchange
    :param market_pair: name of the market pair
    :param interval:
    :param periods: # of latest periods to fetch
    :return: DataFrame object
    """
    with database.lock:
        logger.info("Query latest candle for "+exchange+' '+market_pair+'per '+interval)
        s = select([database.OHLCV]).where(and_(database.OHLCV.c.exchange == exchange,
                                                database.OHLCV.c.symbol == market_pair,
                                                database.OHLCV.c.interval == interval)).order_by(
            database.OHLCV.c.timestamp.desc()).limit(periods)
        result = conn.execute(s)
        latest = pd.DataFrame(result.fetchall())
        latest.columns = result.keys()
        result.close()
        return latest



@retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
def __start_ticker(exchangeInterface,exchange, market_pair,  interval):
    """Start a ticker on its own thread

    exchangeInterface: ExchangeInterface
    exchange: exchange name
    interval: ('1m', '5m', '1h', '6h')

    """

    try:
        if interval not in ['1m','5m','1h','6h']:
            raise ValueError(
                "{} does not support {} timeframe for OHLCV data. Possible values are: {}".format(
                    exchange,
                    interval,
                    ['1m', '5m', '1h', '6h']
                )
            )
    except AttributeError:
        logger.error(
            '%s interface does not support timeframe queries! We are unable to fetch data!',
            exchange
        )
        raise AttributeError(sys.exc_info())


    logger.info(interval + " ticker running...")
    live_tick_count = 0
    while True:
        logger.info("Live Tick: {}".format(str(live_tick_count)))
        print(interval + " tick")
        ohlcv,ticker = exchangeInterface.get_live_data(market_pair,exchange,interval)
        with database.lock:
            ins = database.OHLCV.insert().values (Timestamp = ticker['timestamp'],
                                                 Exchange=exchange,
                                                 Symbol=market_pair,
                                                 Datetime=ticker['datetime'],
                                                 Open=ohlcv[1], High=ohlcv[2], Low=ohlcv[3], Close=ohlcv[4], Volume=ohlcv[5],
                                                 Interval=interval,
                                                 Ask = ticker['ask'],
                                                 Bid = ticker['bid'])
            conn.execute(ins)

        # TODO: run the algorithm & update position & trade

        live_tick_count += 1
        print(ticker['datetime'])

        sleep_time = max(exchangeInterface.exchanges[exchange].rateLimit/1000,__convert_interval_to_int(interval))

        time_.sleep(sleep_time)


def __convert_interval_to_int(interval):
    if interval == "1m":
        return 60
    if interval == "5m":
        return 300
    if interval == "1h":
        return 3600
    if interval == "6h":
        return 21600
    if interval == '1d':
        return 60*60*24

# def convert_timestamp_to_date(timestamp):
#     value = datetime.datetime.fromtimestamp(float(str(timestamp)[:-3]))  #this might only work on bittrex candle timestamps
#     return value.strftime('%Y-%m-%d %H:%M:%S')