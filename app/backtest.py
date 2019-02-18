## NEED TO RUN IN JUPYTER NOTEBOOK

from talib import *
import pandas as pd
import talib
import sys
all_indicator = pd.Series(dir(talib))
cdl = all_indicator[all_indicator.str.startswith("CDL")]

from exchange import ExchangeInterface
from logics.strategies.cdl_test import CDL_Test
import yaml
import pyfolio as pf

def main():
    with open("app/config.yml", 'r') as stream: data_loaded = yaml.load(stream)

    exchangeInterface = ExchangeInterface(data_loaded['exchanges'])
    exchange = list(data_loaded['exchanges'].keys())[0]
    market_pair = data_loaded['settings']['market_pairs'][0]
    interval = data_loaded['settings']['update_interval']
    max_periods = data_loaded['settings']['backtest_periods']
    ohlcv = exchangeInterface.get_historical_data(exchange,market_pair,interval,max_periods)

    first = CDL_Test(ohlcv)


    cdl_list = list(map(lambda x: eval('talib.'+x),cdl))
    params_list={'trailing_window':[10,15],'indicator':cdl_list}
    #result = first.run_algorithm(params_list)
    best_sharpe, params = first.optim_algo(params_list)
    first.optim_grid.sort_values(by='sharpe',ascending=False)
    result = first.run_algorithm(params)



    pf.create_full_tear_sheet(result.returns)


    # https://quantopian.github.io/pyfolio/notebooks/zipline_algo_example/#extract-metrics


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)