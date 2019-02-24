#!/usr/bin/env python3
"""Main app module
"""
import sys
from market import database
import sqlalchemy as db
import pandas as pd
import talib
import ccxt
from exchange import ExchangeInterface
from market import datafeed
import yaml
import time as tm
from logics.strategies.cdl_test import CDL_Test
from logics.strategies.backtest_optim import Backtest_Optim
import logging
from multiprocessing.pool import Pool
logger = logging.getLogger(__name__)


def cdl_back_test(exchangeInterface,exchange,market_pair,interval,data_loaded,max_periods=300,pre_trained=True):
    # using cdl_test as an example
    # search candle indicators in talib for the best-performing indicator

    if pre_trained:
        params = {'trailing_window':data_loaded['strategies']['talib_cdl']['trailing_window'],
              'indicator':getattr(talib,data_loaded['strategies']['talib_cdl']['indicator'])}
    else:
        all_indicator = pd.Series(dir(talib))
        cdl = all_indicator[all_indicator.str.startswith("CDL")]

        ohlcv = exchangeInterface.get_historical_data(exchange, market_pair, interval, max_periods)
        first_strategy = CDL_Test(ohlcv)
        cdl_list = list(map(lambda x: eval('talib.' + x), cdl))
        params_list = {'trailing_window': [10, 15], 'indicator': cdl_list}
        best_sharpe, params = first_strategy.optim_algo(params_list)
        logger.info('Best sharpe: %2f, Best Parameters: %s'%(best_sharpe,params))

    return params


#####################
### STRATEGY
#####################


def start_strategy_(exchange, market_pair, Backtest_Optim, params, interval):
    """
    :param Backtest_Optim: a pre-fitted Backtest_Optim object
    :param params: the best parameters
    :return:
    """
    lookback = params['trailing_window']
    ohlcv_new = datafeed.get_latest_data_from_db(exchange, market_pair, interval, periods=lookback + 1)

    backtest_optim = Backtest_Optim()
    strategy_signal = backtest_optim.refit(ohlcv=ohlcv_new, params=params)
    print(strategy_signal)
    return strategy_signal

# FIXME; only able to use talib now
# strategies are specified in the config file
def start_strategy(exchange, market_pair, interval,strategies):
    buy = []
    sell = []

    def log_result(result):
        # This is called whenever start_strategy_ returns a result
        # result_list is modified only by the main process, not the pool workers.
        buy.append(result['buy'])
        sell.append(result['sell'])

    pool = Pool()
    for key, val in strategies.items():
        Backtest_Optim = globals()[val['backtest_optim']]  # extract the strategy
        assert interval == val['interval'], "strategy is not optimized for the given interval"
        params = {'trailing_window': val['trailing_window'], 'indicator': getattr(talib, val['indicator'])}
        pool.apply_async(start_strategy_, args=(exchange, market_pair, Backtest_Optim, params, interval),
                         callback=log_result)
    pool.close()
    pool.join()
    return {'buy': buy, 'sell': sell}

def main():

    with open("app/config.yml", 'r') as stream:
        data_loaded = yaml.load(stream)

    exchangeInterface = ExchangeInterface(data_loaded['exchanges'])
    exchange = list(data_loaded['exchanges'].keys())[0]
    market_pair = data_loaded['settings']['market_pairs'][0]
    interval = data_loaded['settings']['update_interval']
    strategies = data_loaded['strategies']

    engine = database.engine
    conn = engine.connect()

    params = cdl_back_test(exchangeInterface,exchange,market_pair,interval,data_loaded,max_periods=1000,pre_trained=True)


    database.reset_db()
    # get historical & live data for the strategies
    datafeed.start_ticker(exchangeInterface, exchange, market_pair, interval=interval)

    # start all the strategies
    strategy_result = start_strategy(exchange, market_pair, interval,strategies)
    #####################
    ### RISK MANAGEMENT
    #####################
    # Position control
    # let's conjure up a position book
    fake_position_data = pd.DataFrame({
        'exchange': 'gdax',
        'symbol': 'BTC/USD',
        'position': 'long',
        'amount': 0.1,
        'price': 3600}, index=range(1))

    # TODO: use config file to list all the exchanges, market pair and update interval
    # that we need to pull for position control
    ohlcv_new = datafeed.get_latest_data_from_db(exchange, market_pair, interval)

    # Look at the collected market data
    s = db.select([database.OHLCV])
    result = conn.execute(s)
    data = result.fetchall()
    df = pd.DataFrame(data)
    df.columns = result.keys()

    order_book_raw = exchangeInterface.get_order_book('gdax', 'BTC/USD')

    # purge orders - tbc

    from logics.risk_management import position_control

    position_control_simple = position_control.Position_Control(fake_position_data, ohlcv_new, 0.2, 0.2)
    # produce sell signals only
    rm_result = position_control_simple.control()

    # Order execution
    from logics.risk_management import order_control

    def order_execution(exchangeInterface, exchange, market_pair, position_book, order_book, orderType='market', ):
        if (strategy_result['sell'][0] or rm_result['sell'][0]):
            free_balance = exchangeInterface.get_free_balance(exchange)
            position = \
            position_book.loc[(position_book.exchange == exchange) & (position_book.symbol == market_pair)]['position'][
                0]
            order_control_simple = order_control.Order_Control(exchange, market_pair, free_balance, position,
                                                               position_book, order_book)
            return order_control_simple.simple_control()

    exec_price, exec_size = order_execution(exchangeInterface, exchange, market_pair, fake_position_data,
                                            order_book_raw)

    ###### ACTUAL ORDER EXECUTION
    # exchangeInterface.create_order(exchange,market_pair,'limit','buy',exec_size,exec_price)

    def start_strategy(exchange, market_pair, interval):
        buy = []
        sell = []

        def log_result(result):
            # This is called whenever start_strategy_ returns a result
            # result_list is modified only by the main process, not the pool workers.
            buy.append(result['buy'])
            sell.append(result['sell'])

        pool = Pool()
        for key, val in strategies.items():
            Backtest_Optim = globals()[val['backtest_optim']]  # extract the strategy
            assert interval == val['interval'], "strategy is not optimized for the given interval"
            params = {'trailing_window': val['trailing_window'], 'indicator': getattr(talib, val['indicator'])}
            pool.apply_async(start_strategy_, args=(exchange, market_pair, Backtest_Optim, params, interval),
                             callback=log_result)
        pool.close()
        pool.join()
        return {'buy': buy, 'sell': sell}

    start_strategy(exchange, market_pair, interval)

    order_info = exchangeInterface.exchanges['gdax'].fetch_orders('BTC/USD')[0]

    def log_trade(exchangeInterface, exchange, orderID):
        """
        :param exchangeInterface:
        :param exchange:
        :param market_pair:
        :param orderID:
        :return: True if the trade is logged, False otherwise
        """
        status = None

        timeout = tm.time() + 120

        while status != 'filled' and tm.time() < timeout:
            status = exchangeInterface.get_order_info(exchange, orderID)['status']
            tm.sleep(exchange.rateLimit / 1000)

        if status == 'filled':
            info = exchangeInterface.get_order_info(exchange, orderID)['info']
            with database.lock:
                ins = database.TradeBook.insert().values(info)
                conn.execute(ins)
            return True

        else:
            return False

    orderID = exchangeInterface.create_order()[0]['id']

    log_trade(exchangeInterface, exchange, orderID)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
