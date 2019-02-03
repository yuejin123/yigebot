"""Interface for performing queries against exchange API's
"""

import re
import sys
import time as tm
import datetime as dt
from pytz import timezone
import itertools

from pandas.tseries.offsets import CustomBusinessDay
from trading_calendars import register_calendar, TradingCalendar
from trading_calendars.errors import  CalendarNameCollision
from zipline.utils.memoize import lazyval

import ccxt
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt
import pandas as pd

from market import database

engine = database.engine
conn = engine.connect()

class ExchangeInterface:
    """Interface for performing queries against exchange APIs
    """

    def __init__(self, exchange_config):
        """Initializes ExchangeInterface class

        Args:
            exchange_config (dict): A dictionary containing configuration for the exchanges.
        """

        self.logger = structlog.get_logger()
        self.exchanges = dict()

        # Loads the exchanges using ccxt.
        for exchange in exchange_config:

            config = dict()
            if exchange_config[exchange]['api']['enabled']:
               config =  exchange_config[exchange]['api']['setting']

            if exchange_config[exchange]['required']['enabled']:
                config.update({"enableRateLimit": True})

            new_exchange = getattr(ccxt, exchange)(config)

            # sets up api permissions for user if given
            if new_exchange:
                self.exchanges[new_exchange.id] = new_exchange
            else:
                self.logger.error("Unable to load exchange %s", new_exchange)

    @retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
    def get_live_data(self, exchange,market_pair,  time_unit):
        try:
            if time_unit not in self.exchanges[exchange].timeframes:
                raise ValueError(
                    "{} does not support {} timeframe for OHLCV data. Possible values are: {}".format(
                        exchange,
                        time_unit,
                        list(self.exchanges[exchange].timeframes)
                    )
                )
        except AttributeError:
            self.logger.error(
                '%s interface does not support timeframe queries! We are unable to fetch data!',
                exchange
            )
            raise AttributeError(sys.exc_info())

        ohlcv = self.exchanges[exchange].fetch_ohlcv(
            market_pair,
            timeframe=time_unit,
            limit=1
        )[0]



        if not ohlcv:
            raise ValueError('No historical data provided returned by exchange.')


        ticker_data = self.exchanges[exchange].fetch_ticker(market_pair)

        if not ticker_data:
            raise ValueError('No ticker data provided returned by exchange.')

        return ohlcv, ticker_data

    @retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
    def get_historical_data(self, exchange, market_pair, time_unit, start_date=None, max_periods=1000):
        """
        Get historical OHLCV for a symbol pair

        Decorators:
            retry

        Args:
            exchange (str): Contains the exchange to fetch the historical data from.
            time_unit (str): A string specifying the ccxt time unit i.e. 5m or 1d.
            start_date (int, optional): Timestamp in milliseconds.
            market_pair (str): Contains the symbol pair to operate on i.e. BURST/BTC
            max_periods (int, optional): Defaults to 100. Maximum number of time periods
              back to fetch data for.

        Returns:
            list: Contains a list of lists which contain timestamp, open, high, low, close, volume.
        """

        try:
            if time_unit not in self.exchanges[exchange].timeframes:
                raise ValueError(
                    "{} does not support {} timeframe for OHLCV data. Possible values are: {}".format(
                        exchange,
                        time_unit,
                        list(self.exchanges[exchange].timeframes)
                    )
                )
        except AttributeError:
            self.logger.error(
                '%s interface does not support timeframe queries! We are unable to fetch data!',
                exchange
            )
            raise AttributeError(sys.exc_info())

        if not start_date:
            timeframe_regex = re.compile('([0-9]+)([a-zA-Z])')
            timeframe_matches = timeframe_regex.match(time_unit)
            time_quantity = timeframe_matches.group(1)
            time_period = timeframe_matches.group(2)

            timedelta_values = {
                'm': 'minutes',
                'h': 'hours',
                'd': 'days',
                'w': 'weeks',
                'M': 'months',
                'y': 'years'
            }

            timedelta_args = { timedelta_values[time_period]: int(time_quantity) }

            start_date_delta = dt.timedelta(**timedelta_args)
            now_ = dt.datetime.utcnow().replace(tzinfo=timezone('utc'))
            max_days_date = now_ - (max_periods * start_date_delta)
            start_date = int(max_days_date.astimezone(timezone('UTC')).timestamp() * 1000)

        historical_data = self.exchanges[exchange].fetch_ohlcv(
            market_pair,
            timeframe=time_unit,
            since=start_date
        )

        historical_data.sort()
        historical_data_  = list(k for k,_ in itertools.groupby(historical_data))

        if not historical_data_:
            raise ValueError('No historical data provided returned by exchange.')

        # REST polling

        timeout = tm.time()+60 #CHANGEME the while loop will stop after 60 seconds
        while (len(historical_data_)<max_periods) and (
            int((now_-start_date_delta).timestamp() * 1000)>historical_data_[-1][0]
                ):
            tm.sleep(self.exchanges[exchange].rateLimit / 1000)
            max_periods_ = max_periods-len(historical_data_)
            max_days_date_ = now_ - (max_periods_ * start_date_delta)
            # start_date_= int(max_days_date_.astimezone(timezone('UTC')).timestamp() * 1000)
            start_date_ = int((dt.datetime.fromtimestamp(historical_data_[-1][0]/1000)+start_date_delta).timestamp()*1000)
            print(len(historical_data_))
            print(max_days_date_)
            historical_data = self.exchanges[exchange].fetchOHLCV(
                                              market_pair,
                                              timeframe=time_unit,
                                              since=start_date_
                                          )
            historical_data.sort()
            historical_data  = list(k for k,_ in itertools.groupby(historical_data))
            historical_data_.extend(historical_data)
            if tm.time()>timeout:
                self.logger.info("{} data points are captured".format(len(historical_data_)))
                break

        # Sort by timestamp in ascending order
        historical_data_.sort(key=lambda d: d[0])

        # rateLimit:  request rate limit in milliseconds.
        # Specifies the required minimal delay between two consequent HTTP requests to the same exchanges

        tm.sleep(self.exchanges[exchange].rateLimit / 1000)

        return historical_data_

    # @retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
    # def get_exchange_markets(self, exchanges=[], markets=[]):
    #     """Get market information for all symbol pairs listed on all api-enabled exchanges.
    #
    #     Args:
    #         markets (list, optional): A list of markets to get from the exchanges. Default is all
    #             markets.
    #         exchanges (list, optional): A list of exchanges to collect information from. Default is
    #             all enabled exchanges.
    #
    #     Decorators:
    #         retry
    #
    #     Returns:
    #         dict: A dictionary containing market information for all symbol pairs.
    #     """
    #
    #     if not exchanges:
    #         exchanges = self.exchanges
    #
    #     exchange_markets = dict()
    #     for exchange in exchanges:
    #
    #         exchange_markets[exchange] = self.exchanges[exchange].load_markets()
    #
    #         if markets:
    #             curr_markets = exchange_markets[exchange]
    #
    #             # Only retrieve markets the users specified
    #             exchange_markets[exchange] = { key: curr_markets[key] for key in curr_markets if key in markets }
    #
    #             for market in markets:
    #                 if market not in exchange_markets[exchange]:
    #                     self.logger.info('%s has no market %s, ignoring.', exchange, market)
    #
    #         tm.sleep(self.exchanges[exchange].rateLimit / 1000)
    #
    #     return exchange_markets

    @retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
    def get_order_book(self, exchange,market_pair):
        """
        Get order book for a symbol pair

        Decorators:
            retry

        Args:
            exchange (str): Contains the exchange to fetch the historical data from.
            market_pair (str): Contains the symbol pair to operate on i.e. BURST/BTC
        Returns:
            list: Contains a dict of DataFrame which contain 'bids' and 'spreads', and each data frame contains 'price' and 'volume'
        """

        order_book_raw = self.exchanges[exchange].fetch_order_book(market_pair)
        order_book = {'bids': pd.DataFrame({'price': [i[0] for i in order_book_raw['bids']],
                                            'volume': [i[1] for i in order_book_raw['bids']]}),
                      'asks': pd.DataFrame({'price': [i[0] for i in order_book_raw['asks']],
                                            'volume': [i[1] for i in order_book_raw['asks']]})
                      }

        if not order_book:
            raise ValueError("No order book data returned by the exchange")

        return order_book

    @retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
    def get_free_balance(self, exchange,symbol='USD'):
        """
        Get free balance for the account within the exchange

        :param exchange: string or list of string, exchange to query the balance
        :param symbol: string, symbol to query
        :return: balance
        """
        free = None
        if self.exchanges[exchange].has['fetchBalance']:
            free = self.exchanges[exchange].fetchBalance()[symbol]['free']

        if free is None:
            raise ValueError("No " + symbol +" balance data returned by the exchange")

        return free

    @retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
    def create_order(self, exchange, market_pair, type, side, amount, price=None, **kwargs):
        """
        :param exchange:
        :param market_pair:
        :param type: order type, 'market' or 'limit'
        :param side: 'buy' or 'sell'
        :param amount: numeric, number of shares to buy or sell
        :param price: optional, price at which to place the order; optional depending on the order type
        :param kwargs: customized order parameters for overriding order types.
        https://github.com/ccxt/ccxt/wiki/Manual#overriding-unified-api-params.

        :return:
        """
        order = None
        if type == 'market':
            if side == 'buy':
                if self.exchanges[exchange].has['create_market_buy_order']:
                    order = self.exchanges[exchange].create_market_buy_order(market_pair, amount)
            elif side == 'sell':
                if self.exchanges[exchange].has['create_market_sell_order']:
                    order = self.exchanges[exchange].create_market_sell_order(market_pair, amount)
            else:
                self.logger.error("Invalid order: %s", side)

        elif type == 'limit':
            if side == 'buy':
                if self.exchanges[exchange].has['create_limit_buy_order']:
                    order = self.exchanges[exchange].create_limit_buy_order(market_pair, amount, price)
            elif side == 'short':
                if self.exchanges[exchange].has['create_limit_sell_order']:
                    order = self.exchanges[exchange].create_limit_sell_order(market_pair, amount, price)
            else:
                self.logger.error("Invalid order: %s", side)

        else:
            order = self.exchanges[exchange].createOrder(market_pair, type, side, amount, price, **kwargs)

        position = 'long' if side == 'buy' else 'short'

        with database.lock:
            ins = database.OrderBook.insert().values(timestamp=order['timestamp'],
                                                     datetime=order['datetime'],
                                                     orderID=order['id'],
                                                     orderType=order['type'],
                                                     exchange=exchange,
                                                     symbol=order['symbol'],
                                                     position=position,
                                                     amount=order['amount'],
                                                     price=price)
            conn.execute(ins)
        tm.sleep(self.exchanges[exchange].rateLimit / 1000)
        return order

    @retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
    def get_order_info(self, exchange,orderID):
        """
        Get order info using order ID and exchange as reference

        :param exchange: string or list of string, exchange to query the balance
        :param orderID: string, order ID
        :return: order status and trade information related to the order if the order is closed.
        """
        my_trade_keys = ["timestamp","datetime","id","order","amount","price","cost"]
        trade_book_columns =  ['timestamp','datetome','tradeID','orderID','amount','price','cost','fee']
        trade_info = None
        status=None
        if self.exchanges[exchange].has['fetchOrder']:
            order_info = self.exchanges[exchange].fetch_order(orderID)
            status = order_info['status']
        if not status: raise ValueError('The exchange does not return order status')

        elif status !='closed': return {'status':status, 'info':trade_info}
        else:
            my_trades =  self.exchanges[exchange].fetch_my_trades(order_info['symbol'])
            right_trades = list(filter(lambda a: a['order']==orderID,my_trades))
            trade_info = [{info[0]:trade[info[1]] for info in zip(trade_book_columns,my_trade_keys)} for trade in right_trades]
            return {'status': status,'info':trade_info}
        tm.sleep(self.exchanges[exchange].rateLimit / 1000)


class TFSExchangeCalendar(TradingCalendar):
    """
    An exchange calendar for trading assets 24/7.

    Open Time: 12AM, UTC
    Close Time: 11:59PM, UTC
    """

    @property
    def name(self):
        """
        The name of the exchange, which Zipline will look for
        when we run our algorithm and pass TFS to
        the --trading-calendar CLI flag.
        """
        return "TFS"

    @property
    def tz(self):
        """
        The timezone in which we'll be running our algorithm.
        """
        return timezone("UTC")

    @property
    def open_time(self):
        """
        The time in which our exchange will open each day.
        """
        return dt.time(0,0)

    @property
    def close_time(self):
        """
        The time in which our exchange will close each day.
        """
        return dt.time(23, 59)

    @lazyval
    def day(self):
        """
        The days on which our exchange will be open.
        """
        weekmask = "Mon Tue Wed Thu Fri Sat Sun"
        return CustomBusinessDay(
            weekmask=weekmask
        )


start_session = pd.Timestamp('2000-01-07', tz='utc')
end_session = pd.Timestamp('2099-12-31', tz='utc')
try:
    register_calendar(
        'TFS',
        TFSExchangeCalendar(start=start_session, end=end_session)
    )
except CalendarNameCollision:
    pass