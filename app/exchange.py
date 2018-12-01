"""Interface for performing queries against exchange API's
"""

import re
import sys
import time as tm
from datetime import datetime, timedelta,time
from pytz import timezone
import itertools

from pandas.tseries.offsets import CustomBusinessDay
from trading_calendars import register_calendar, TradingCalendar
from zipline.utils.memoize import lazyval

import ccxt
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt
import pandas as pd

class ExchangeInterface:
    """Interface for performing queries against exchange API's
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
    def get_live_data(self, market_pair, exchange, time_unit):
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
    def get_historical_data(self, market_pair, exchange, time_unit, start_date=None, max_periods=1000):
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

            start_date_delta = timedelta(**timedelta_args)
            now_ = datetime.utcnow().replace(tzinfo=timezone('utc'))
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
        while (len(historical_data_)<max_periods) and (
            int((now_-start_date_delta).timestamp() * 1000)>historical_data_[-1][0]
                ):
            tm.sleep(self.exchanges[exchange].rateLimit / 1000)
            max_periods_ = max_periods-len(historical_data_)
            max_days_date_ = now_ - (max_periods_ * start_date_delta)
            # start_date_= int(max_days_date_.astimezone(timezone('UTC')).timestamp() * 1000)
            start_date_ = int((datetime.fromtimestamp(historical_data_[-1][0]/1000)+start_date_delta).timestamp()*1000)
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

        # Sort by timestamp in ascending order
        historical_data_.sort(key=lambda d: d[0])

        # rateLimit:  request rate limit in milliseconds.
        # Specifies the required minimal delay between two consequent HTTP requests to the same exchange

        tm.sleep(self.exchanges[exchange].rateLimit / 1000)

        return historical_data_


    @retry(retry=retry_if_exception_type(ccxt.NetworkError), stop=stop_after_attempt(3))
    def get_exchange_markets(self, exchanges=[], markets=[]):
        """Get market information for all symbol pairs listed on all api-enabled exchanges.

        Args:
            markets (list, optional): A list of markets to get from the exchanges. Default is all
                markets.
            exchanges (list, optional): A list of exchanges to collect information from. Default is
                all enabled exchanges.

        Decorators:
            retry

        Returns:
            dict: A dictionary containing market information for all symbol pairs.
        """

        if not exchanges:
            exchanges = self.exchanges

        exchange_markets = dict()
        for exchange in exchanges:

            exchange_markets[exchange] = self.exchanges[exchange].load_markets()

            if markets:
                curr_markets = exchange_markets[exchange]

                # Only retrieve markets the users specified
                exchange_markets[exchange] = { key: curr_markets[key] for key in curr_markets if key in markets }

                for market in markets:
                    if market not in exchange_markets[exchange]:
                        self.logger.info('%s has no market %s, ignoring.', exchange, market)

            tm.sleep(self.exchanges[exchange].rateLimit / 1000)

        return exchange_markets


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
        return time(0,0)

    @property
    def close_time(self):
        """
        The time in which our exchange will close each day.
        """
        return time(23, 59)

    @lazyval
    def day(self):
        """
        The days on which our exchange will be open.
        """
        weekmask = "Mon Tue Wed Thu Fri Sat Sun"
        return CustomBusinessDay(
            weekmask=weekmask
        )


start_session = pd.Timestamp('2012-01-07', tz='utc')
end_session = pd.Timestamp('2018-11-13', tz='utc')

register_calendar(
    'TFS',
    TFSExchangeCalendar(start=start_session, end=end_session)
)