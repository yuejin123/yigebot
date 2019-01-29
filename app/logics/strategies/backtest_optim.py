from zipline.api import order, record, symbol
from zipline.finance import commission, slippage
# Import exponential moving average from talib wrapper
from talib import EMA
from talib import BBANDS
from collections import OrderedDict
from datetime import datetime
import pandas as pd

from exchange import TFSExchangeCalendar
import zipline
import pandas as pd

class Backtest_Optim:


    """
    A ema/bb strategy example
    """


    def __init__(self,ohlcv,asset_symbol='BTC',frequency='daily'):
        """
        Args:
        ohlcv: returns from ccxt.exchange.fetch_ohlcv()
        frequency: {'daily', 'minute'}, optional) – The data frequency to run the algorithm at.

        """

        def convert_to_dataframe(historical_data):
            """Converts historical data matrix to a pandas dataframe.

            Args:
                historical_data (list): A matrix of historical OHCLV data.

            Returns:
                pandas.DataFrame: Contains the historical data in a pandas dataframe.
            """

            dataframe = pd.DataFrame(historical_data)
            dataframe.transpose()

            dataframe.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            dataframe['datetime'] = dataframe.timestamp.apply(
                lambda x: pd.to_datetime(datetime.fromtimestamp(x / 1000).strftime('%c'))
            )

            dataframe.set_index('datetime', inplace=True, drop=True)
            dataframe.drop('timestamp', axis=1, inplace=True)

            return dataframe

        self.asset_symbol = asset_symbol
        self.frequency = frequency
        ohlcv_df = convert_to_dataframe(ohlcv)
        data = OrderedDict()
        temp = ohlcv_df
        # FIXME: only works for daily frequency data...
        if frequency =='daily':
            temp.index = list(map(lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0), ohlcv_df.close.index))
        data[self.asset_symbol] = temp
        #TODO: Panel is deprecated, Panel data might be discarded in later version of zipline
        self.panel = pd.Panel(data)
        self.panel.minor_axis = ['open', 'high', 'low', 'close', 'volume']

    def initialize_(self,params_list,commission_cost,*args,**kwargs):
        """

        :param params_list: dict, parameters used for strategies
        :param commision_cost: dict, commicion cost setting


        :return: a complete run_algorithm function to be run
        """
        asset_symbol = self.asset_symbol


        def initialize(context):
            context.asset = symbol(asset_symbol)

            # To keep track of whether we invested in the stock or not
            context.invested = False

            # Explicitly set the commission/slippage to the "old" value until we can
            context.set_commission(commission.PerShare(**commission_cost))
            context.set_slippage(slippage.VolumeShareSlippage())
            context.params = params_list
        self.initialize = initialize
        return initialize

    def handle_data_(self,handle_data_func = None):
        """
        Need to be customized for every strategy


        :param handle_data_func: optional. If a ready-to-go handle_data function is available
        :return: a handle_data function ready to be passed into run_algorithm
        """
        if handle_data_func:
            self.handle_data = handle_data_func
            return handle_data_func
        else:

            def handle_data(context, data):
                required_params = ['trailing_window','ema_s','ema_l','bb']
                if not all(param in context.params for param in required_params):
                    raise KeyError("incorrect parameter list")
                trailing_window = data.history(context.asset, 'close', context.params['trailing_window'], '1d')
                if trailing_window.isnull().values.any():
                    return
                ema_s = EMA(trailing_window.values, timeperiod=context.params['ema_s'])
                ema_l = EMA(trailing_window.values, timeperiod=context.params['ema_l'])
                bb = BBANDS(trailing_window.values,timeperiod = context.params['bb'])


                buy = False
                sell = False
                buy_signal = (ema_s[-1] > ema_l[-1]) and (trailing_window.values[-1]>(bb[1][-1])) and  (trailing_window.values[-1]>ema_s[-1])

                if buy_signal and not context.invested:
                    order(context.asset, 100)
                    context.invested = True
                    buy = True

                elif not buy_signal and context.invested:
                    order(context.asset, -100)
                    context.invested = False
                    sell = True

                record(BTC=data.current(context.asset, "price"),
                       ema_s=ema_s[-1],
                       ema_l=ema_l[-1],
                       bb = bb[1][-1],
                       buy=buy,
                       sell=sell)

            self.handle_data= handle_data

            return handle_data


    def run_algorithm(self,params_list,capital_base=800000,exchange_calendar=TFSExchangeCalendar(),**kwargs):
        #TODO: sortino ratio warning, suppress?
        """

        :param params_list: list of parameter to be used for the strategy
        :param capital_base: optional. Money to start with
        :param exchange_calendar: TradingCalendar
        :return: return from zipline.run_algorithm()
        """



        if 'trailing_window' not in params_list:
            raise KeyError('data history parameter missing')

        if self.panel[self.asset_symbol].index[params_list['trailing_window']].tzinfo:
            self.start_session = self.panel[self.asset_symbol].index[params_list['trailing_window']].tz_convert('UTC').to_pydatetime()
            self.end_session = self.panel[self.asset_symbol].index[-1].tz_convert('utc').to_pydatetime()
        else:
            self.start_session= self.panel[self.asset_symbol].index[params_list['trailing_window']].tz_localize('utc').to_pydatetime()
            self.end_session = self.panel[self.asset_symbol].index[-1].tz_localize('utc').to_pydatetime()

        result = zipline.run_algorithm(start = self.start_session,\
                      end = self.end_session,\
                      initialize = self.initialize_(params_list=params_list,commission_cost={'cost':0.0075}), \
                      handle_data= self.handle_data_(),\
                      data = self.panel,\
                      capital_base=capital_base,\
                      data_frequency = self.frequency,\
                      trading_calendar=exchange_calendar,**kwargs,)


        return result

    def optim_algo(self,params_grid):
        """
        Optimize strategy performance measured sharpe ratio

        :param params_grid: dictionary or list of dictionaries of parameters to test the performance on
        :return: the best sharpe ratio and the corresponding parameters
        """
        from sklearn.model_selection import ParameterGrid
        from numpy import Infinity
        """
        Args:
        params_grid: dict or list of dictionaries
            Dictionary with parameters names (string) as keys and lists of
            parameter settings to try as values, or a list of such
            dictionaries, in which case the grids spanned by each dictionary
            in the list are explored. This enables searching over any sequence
            of parameter settings.
        """

        grid = ParameterGrid(params_grid)
        self.optim_grid = pd.DataFrame.from_dict([i for i in grid])
        max_sharpe = -Infinity
        sharpe_list=[]
        for params in grid:
            if (params["ema_s"]>params['ema_l']) or (max(params.values())>params['trailing_window']):
                sharpe=-Infinity
                sharpe_list.append(sharpe)
                continue
            perf = self.run_algorithm(params_list=params)
            sharpe = perf.sharpe[-1]
            sharpe_list.append(sharpe)
            if sharpe > max_sharpe:
                max_sharpe = sharpe
                best = params

        self.optim_grid['sharpe']=sharpe_list
        return max_sharpe,best


    def refit(self,ohlcv,params = None, ba = None,**kwargs):
        """
        :param ohlcv: a DataFrame object with OHLCV columns ordered by date, ascending
        :param params: optimal parameters
        :ba: bid ask spread
        :return: signal
        """

        required_params = ['trailing_window', 'ema_s', 'ema_l', 'bb']
        if not all(param in params for param in required_params):
            raise KeyError("incorrect parameters")
        lookback = params['trailing_window']
        close = ohlcv['close']
        ema_s = EMA(close[-lookback:], timeperiod=params['ema_s'])
        ema_l = EMA(close[-lookback:], timeperiod=params['ema_l'])
        bb = BBANDS(close[-lookback:], timeperiod=params['bb'])
        if ba in None:
            buy_signal = (ema_s > ema_l) and (close[-1] > (bb[1])) and (
                    close[-1] > ema_s)
        else:
            mid = (bid[-1]+ask[-1])/2
            buy_signal = (ema_s > ema_l) and (mid > (bb[1])) and (
                    mid > ema_s)


        return {'buy':buy_signal,'sell': None}