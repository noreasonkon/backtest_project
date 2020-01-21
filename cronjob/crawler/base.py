import datetime
import sqlite3
import pandas as pd
from settings import DB_SETTING


class BaseCrawler(object):

    def __init__(self):
        self.market = ''
        self.symbol = ''

    def connect(self):
        db_setting = DB_SETTING
        if db_setting['DB_TYPE'] == 'sqlite':
            return sqlite3.connect(db_setting['NAME'],
                                   check_same_thread=False)
        else:
            raise NotImplementedError()

    def request(self, *args, **kwargs):
        raise NotImplementedError()

    def parse(self, *args, **kwargs):
        raise NotImplementedError()

    def save(self, *args, **kwargs):
        raise NotImplementedError()
