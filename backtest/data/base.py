import sqlite3
import pandas as pd
import os
import datetime
import talib
from talib import abstract

class BaseData():
    def __init__(self, symbol):
        self.conn = sqlite3.connect(os.path.join('data', "data.sqlite3"))
        self.symbol = symbol
        cursor = self.conn.execute(
            'SELECT name FROM sqlite_master WHERE type = "table"')
        table_names = [t[0] for t in list(cursor)]
        self.col2table = {}
        for tname in table_names:
            c = self.conn.execute('PRAGMA table_info(' + tname + ');')
            for cname in [i[1] for i in list(c)]:
                self.col2table[cname] = tname
        self.date = datetime.datetime.now().date()

    def get_price(self):
        
        s = ("""SELECT date, open,
            high, low, close, volume
            FROM ft_stock_price
            WHERE symbol = '%s'
            ORDER BY date""" % (
            str(self.symbol)))
        self.price = pd.read_sql(s, self.conn, index_col = ['date'])
        return self.price

    def get_rev(self):
        
        s = ("""SELECT date, revenue
            FROM ft_stock_revenue
            WHERE symbol = '%s'
            ORDER BY date""" % (
            str(self.symbol)))
        return pd.read_sql(s, self.conn, index_col = ['date'])

    def get_kd(self):
        kd = abstract.STOCH(self.price)
        return kd