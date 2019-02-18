# control the order size, speed and execution

import pandas as pd
import logging
logger = logging.getLogger(__name__)

class Order_Control:
    def __init__(self,exchange,symbol,free_balance, type, position_data, order_book,price=None,size = None):
        """
        :param exchange:
        :param symbol:
        :param free_balance: free balance on the account
        :param type: 'long' or 'short'
        :param position_data: DataFrame object, position data for the symbol, [exchange,symbol, position, amount, price, cost]
        :param order_book: dict, [bids,asks], in each item, a DataFrame object with [price,volume]
        :param price: optional; the target price if required by the order type
        :param size: optional; the number of shares to buy
        """
        self.exchange = exchange
        self.symbol = symbol
        self.free = free_balance
        self.type = type
        self.position = position_data
        self.order_book = order_book
        self.price = price
        self.size= size
    def _simple_price_control(self):
        if self.price is not None: return self.price
        else:
            mid = 0.5 * self.order_book['bids'].iloc[0, 0] + 0.5 * self.order_book['asks'].iloc[0, 0]
            if self.type == 'long': self.price = mid
            else: self.price = mid
            return self.price

    def _simple_size_control(self):
        if self.size is not None: return self.size
        else:
            if self.position is None:
                # no position built for the asset yet
                if self.type=='long':
                    self.size = 0.25*self.free/self.price
                else:
                    raise ValueError("Let's not short sell <3")
            else:
                # position already built
                if self.type=='long':
                    self.size = min(self.position['amount'].sum(),0.5*self.free/self.price)
                else:
                    self.size = 0.5*(self.position['amount'].sum())
            return self.size


    def simple_control(self):
        self._simple_price_control()
        self._simple_size_control()
        logger.info(
            "Using simple order control to " + self.type + ' ' + str(self.size) + ' shares of ' + self.symbol + ' at ' + str(self.price) + ' in ' + self.exchange)
        return self.price,self.size
