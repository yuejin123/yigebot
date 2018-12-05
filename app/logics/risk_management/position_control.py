from market import database
from threading import Thread
import pandas as pd
import logging
logger = logging.getLogger(__name__)



class Position_Control:
    def __init__(self,position_data, ohlcv, target,loss):
        """

        :param position_data: DataFrame object, [timestamp, datetime, exchange, symbol, position, amount, price, cost]
        :param ohlcv: DataFrame, latest candlestick for each pair of asset, [timestamp, exchange,symbol, datetime, open, high, low, close, volume,interval, bid, ask]
        :param target: profit target
        :param loss: stop-loss limit
        """

        self.position = position_data[['exchange','symbol','position','amount','price']]
        self.ohlcv = ohlcv[['exchange','symbol','open','high','low','close','volume','bid','ask']]
        self.combined = pd.merge(self.position,self.ohlcv,how='left',on=['exchange','symbol'])
        self.target = target
        self.stop_loss = loss

    def __simple_control(self,position_ohlcv):

        logger.info("Using simple position control with profit target " + str(self.target) + 'and stop loss limit ' + self.stop_loss)
        sell = False
        if position_ohlcv['position']=='long':
            # profit target
            if position_ohlcv['ask']/position_ohlcv['price']>=self.target+1: sell=True
            # stop loss
            if position_ohlcv['ask']/position_ohlcv['price']<=1-self.stop_loss: sell=True
        else:
            # in the case of short positions:
            # profit target
            if position_ohlcv['bid'] / position_ohlcv['price'] <= 1-self.target: sell = True
            # stop loss
            if position_ohlcv['bid'] / position_ohlcv['price'] >= self.stop_loss: sell = True

        return sell

    def control(self,control_method,position_ohlcv,*args,**kwargs):
        """

        :param control_method: a private function that specifies the details about controlling the position
        :param position_ohlcv: DataFrame object, the data frame with information about the position and the market information
        :param args:
        :param kwargs:
        :return: position_ohlcv with an additional column 'sell'
        """
        if isinstance(position_ohlcv,pd.Series):
            sell = control_method(position_ohlcv)
            result = position_ohlcv['sell']=sell
        if isinstance(position_ohlcv,pd.DataFrame):
            sell = position_ohlcv.apply(control_method,axis=1)
            result = position_ohlcv['sell']=sell

        return result


