import logging
import sys
import ccxt
from tenacity import retry, retry_if_exception_type, stop_after_attempt
from sqlalchemy import select, and_
logger = logging.getLogger(__name__)

# Pull data from the exchange at a given interval and write to the database

from market import database
from threading import Thread
import time as time_
import pandas as pd

engine = database.engine
conn = engine.connect()


tickers={}
def start_ticker(exchangeInterface,exchange, market_pair='BTC/USD',  interval='1h'):
    """Start a ticker/timer that notifies market watchers when to pull a new candle"""
    tickers[interval] = Thread(\
            target=__start_ticker, args=(exchangeInterface,exchange,market_pair,interval,),name='start_ticker').start()

def get_latest_data_from_db(exchange,market_pair, interval,periods = 1):
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
def __start_ticker(exchangeInterface,exchange, market_pair, interval,backfill=300):
    """Start a ticker on its own thread

    exchangeInterface: ExchangeInterface
    exchange: exchange name
    interval: ('1m', '5m', '1h', '6h')
    backfill: number of tickers to backfiil
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

    print("Get the latest {} tickers".format(str(backfill)))
    logger.info("Get the latest {} tickers".format(str(backfill)))
    hist_ohlcv = exchangeInterface.get_historical_data(exchange,market_pair, interval, max_periods=backfill)
    ohlcv_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    ohlcv_info = []
    for record in hist_ohlcv:
        db_record = dict()
        db_record.update({ohlcv_columns[i]: record[i] for i in range(len(ohlcv_columns))})
        db_record.update({'exchange': exchange, 'symbol': market_pair, 'interval': interval})
        ohlcv_info.append(db_record)

    with database.lock:
        ins = database.OHLCV.insert()
        conn.execute(ins, ohlcv_info)

    logger.info(interval + " ticker running...")
    live_tick_count = 0
    while True:

        print("Live Tick: {}".format(str(live_tick_count)))
        logger.info("Live Tick: {}".format(str(live_tick_count)))
        print(interval + " tick")
        ohlcv,ticker = exchangeInterface.get_live_data(exchange,market_pair,interval)
        with database.lock:
            ins = database.OHLCV.insert().values (timestamp = ticker['timestamp'],
                                                 exchange=exchange,
                                                 symbol=market_pair,
                                                 datetime=ticker['datetime'],
                                                 open=ohlcv[1], high=ohlcv[2], low=ohlcv[3], close=ohlcv[4], volume=ohlcv[5],
                                                 interval=interval,
                                                 ask = ticker['ask'],
                                                 bid = ticker['bid'])
            conn.execute(ins)

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


