from talib import RSI
from talib import MFI
from talib import OBV
from talib import BBANDS

import logics.strategies.backtest_optim
from logics.strategies.backtest_optim import Backtest_Optim

class CDL_Test(Backtest_Optim):

    def __init__(self, ohclv, symbol='BTC', frequency='daily'):
        super().__init__(ohclv,symbol,frequency)

    # override the handle_data_ method for different strategie
    def handle_data_(self,handle_data_func = None):
        """
        The handle data function for testing candle patterns

        :param handle_data_func: optional. If a ready-to-go handle_data function is available
        :return: a handle_data function ready to be passed into run_algorithm
        """
        if handle_data_func:
            self.handle_data = handle_data_func
            return handle_data_func
        else:

            def handle_data(context, data) :
                required_params = ['trailing_window','indicator']
                if not all(param in context.params for param in required_params):
                    raise KeyError("incorrect parameter list")
                trailing_window = data.history(context.asset, ['open','high','low','close'], context.params['trailing_window'], '1d')
                if trailing_window.isnull().values.any():
                    return
                cdl_indicator = context.params['indicator']
                candle_pattern = cdl_indicator(**trailing_window.to_dict(orient='series'))


                buy = False
                sell = False
                buy_signal = candle_pattern[-1]>0
                sell_sigal = candle_pattern[-1]<=0

                #TODO: add more trading control
                if buy_signal and not context.invested:
                    order(context.asset, 100)
                    context.invested = True
                    buy = True

                elif sell_sigal and context.invested:
                    order(context.asset, -100)
                    context.invested = False
                    sell = True

                record(BTC=data.current(context.asset, "price"),
                       ema_s=candle_pattern[-1],
                       buy=buy,
                       sell=sell)
            self.handle_data= handle_data
            return handle_data

    def optim_algo(self,params_grid):
        """
        Customized for each strategy

        :param params_grid: dictionary or list of dictionaries of parameters to test the performance on
        :return: the best sharpe ratio and the corresponding parameters
        """
        from sklearn.model_selection import ParameterGrid
        from numpy import Infinity
        import pandas as pd
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
            perf = self.run_algoritm_(params_list=params)
            sharpe = perf.sharpe[-1]
            sharpe_list.append(sharpe)
            # nothing happened
            if sharpe is None: continue
            elif sharpe > max_sharpe:
                max_sharpe = sharpe
                best = params

        self.optim_grid['sharpe']=sharpe_list
        return max_sharpe,best


