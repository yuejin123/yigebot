import os
import sqlalchemy as db
from threading import Lock
import logging

logger = logging.getLogger(__name__)



db_name = 'yigebot.db'
db_fullpath = os.path.join(
    os.path.dirname(
        os.path.realpath(__file__)),
    db_name)

lock = Lock()
engine = db.create_engine(
    'sqlite:///{}'.format(db_fullpath),
    connect_args={
        'check_same_thread': False},
    echo=False)
metadata = db.MetaData()

#
OHLCV = db.Table('OHLCV', metadata,
              db.Column('timestamp', db.Integer,primary_key=True),
              db.Column('exchange', db.String),
              db.Column('symbol', db.String),
              db.Column('datetime', db.String),
              db.Column('open', db.Float),
              db.Column('high', db.Float),
              db.Column('low', db.Float),
              db.Column('close', db.Float),
              db.Column('volume', db.Float),
              db.Column('interval', db.String),
              db.Column('bid',db.Float),
              db.Column('ask',db.Float)
              )


OrderBook = db.Table('OrderBook', metadata,
                    db.Column('timestamp',db.Integer),
                    db.Column('datetime', db.String),
                    db.Column('orderID', db.String, primary_key=True),
                    db.Column('orderType',db.String),
                    db.Column('exchange', db.String),
                    db.Column('symbol', db.String),
                    db.Column('position', db.String),
                    db.Column('amount', db.Float), # ordered amount of base currency
                    db.Column('price', db.Float),extend_existing=True) # float price in quote currency

TradeBook = db.Table('TradeBook',metadata,
                     db.Column('tradeID',db.String,primary_key=True),
                     db.Column('timestamp',db.Integer),
                     db.Column('datetime',db.String),
                     db.Column('orderID',db.String,db.ForeignKey('OrderBook.orderID')),
                     db.Column('amount', db.Float),
                     db.Column('price', db.Float),
                     db.Column('cost',db.Float),extend_existing=True) #total cost (including fees), `price * amount`

Position = db.Table('Position',metadata,
                    db.Column('timestamp',db.Integer),
                    db.Column('datetime',db.String),
                    db.Column('exchange', db.String,primary_key=True),
                    db.Column('symbol', db.String,primary_key=True),
                    db.Column('position', db.String),
                    db.Column('amount', db.Float),
                    db.Column('price', db.Float),
                    db.Column('cost',db.Float),extend_existing=True)


def drop_tables():
    print('Dropping tables...')
    metadata.drop_all(engine)


def create_tables():
    metadata.create_all(engine)


def reset_db():
    print('Resetting database...')
    drop_tables()
    create_tables()
