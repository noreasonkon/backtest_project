import sqlite3
from settings import DB_SETTING


class BaseTest(object):
    
    def __init__(self):
        self.market = ''
        self.symbol = ''

    def connect(self):

    def check_lastdate(self):
        s = 'select max(date) from xxx'
        last_date = pd.read_sql(s, con=self.conn)
        ...
        return str(datetime.date.today()) == last_date