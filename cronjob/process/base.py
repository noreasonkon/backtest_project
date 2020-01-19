import sqlite3
from settings import DB_SETTING


class BaseProcess(object):

    def connect(self):
        db_setting = DB_SETTING
        if db_setting['DB_TYPE'] == 'sqlite':
            return sqlite3.connect(db_setting['NAME'],
                                   check_same_thread=False)
        else:
            raise NotImplementedError()
