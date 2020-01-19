# coding=utf-8
import requests
import datetime
from io import StringIO
import pandas as pd
from crawler.base import BaseCrawler
from common import utils
import math


class DataDailyIPOstockCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self.table_name = 'data_dailystock'
        self.run_date = datetime.date.today()
        self.start_date = self.run_date
        self.end_date = self._get_last_date(table=self.table_name)

    def request(self, *args, **kwargs):
        url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX'
        resp = requests.get(url, params={
            "date": self.run_date.strftime('%Y%m%d'),
            "response": "csv",
            "type": "ALLBUT0999"
        })
        return resp

    def parse(self, resp, *args, **kwargs):
        df = pd.read_csv(StringIO("\n".join(
            [i.translate({ord(c): None for c in ' '})
             for i in resp.text.split('\n')
             if len(i.split('",')) == 17])), header=0)
        for column in df.columns:
            df[column] = df[column].astype(str)
            df[column] = df[column].str.replace(",", "")
        df['證券代號'] = df['證券代號'].str.zfill(4)
        df['證券代號'] = df['證券代號'].str.replace("=", "")
        df['證券代號'] = df['證券代號'].str.replace('"', '')
        df['漲跌(+/-)'] = df['漲跌(+/-)'].str.replace('nan', '')
        df['漲跌價差'] = df['漲跌(+/-)'] + df['漲跌價差']
        df['漲跌價差'] = df['漲跌價差'].str.replace('+', '')
        df['漲跌價差'] = df['漲跌價差'].str.replace('X', '')
        df['listed'] = "1"
        df['成交股數'] = (pd.to_numeric(df['成交股數'],
                                    downcast='integer')/1000).round(0)
        df['date'] = str(self.run_date)
        for i, title in enumerate(df['證券代號']):
            if (df['收盤價'][i].strip() == "--"):
                df = df.drop([i])
        df = df.reset_index(drop=True)
        return df

    def save(self, df, *args, **kwargs):
        conn = self.connect()
        curs = conn.cursor()
        item = {'date': df["date"],
                'stock_id': df["證券代號"],
                'stock_name': df["證券名稱"],
                'volume': df["成交股數"],
                'count_v': df["成交筆數"],
                'listed': df["listed"],
                'open': df["開盤價"],
                'high': df["最高價"],
                'low': df["最低價"],
                'close': df["收盤價"],
                'ud': df["漲跌(+/-)"],
                'df_close': df["漲跌價差"],
                'last_buy_p': df["最後揭示買價"],
                'last_buy_v': df["最後揭示買量"],
                'last_sell_p': df["最後揭示賣價"],
                'last_sell_v': df["最後揭示賣量"],
                'per': df["本益比"]
                }
        ins = self._insert_sql(self.table_name, item)
        for idx in df.index:
            values = [x[idx] for x in item.values()]
            curs.execute(ins, values)
        conn.commit()
        conn.close()


class DataDailyOTCstockCrawler(BaseCrawler):
    def __init__(self):
        self.table_name = 'data_dailystock'
        self.run_date = datetime.date.today()
        self.start_date = self.run_date
        self.end_date = self._get_last_date()

    def _get_last_date(self):
        conn = self.connect()
        sql_cmd = "select max(`date`) from `%s` limit 1" % self.table_name
        df = pd.read_sql(sql=sql_cmd, con=conn)
        return datetime.datetime.strptime(df.values[0][0], '%Y-%m-%d').date()

    def request(self, *args, **kwargs):
        url = '''http://www.tpex.org.tw/web/stock/aftertrading/''' + \
              '''daily_close_quotes/stk_quote_download.php'''
        start, end = map(lambda d: d.strftime('%Y/%m/%d'),
                         utils.get_period(year=self.run_date.year,
                                          month=self.run_date.month))
        resp = requests.get(url, params={
            "d": utils.to_ROCdate(self.run_date),
            "se": "2,asc,0",
            "l": "zh-tw"
        })
        return resp

    def parse(self, resp, *args, **kwargs):
        lines = resp.text.replace(" ", "")
        lines = lines.split('\n')
        df = pd.read_csv(StringIO("\n".join(lines[3:])), header=None)
        df.columns = list(map(lambda l: l.replace(' ', ''),
                              lines[2].split(',')))
        for column in df.columns:
            df[column] = df[column].astype(str)
            df[column] = df[column].str.replace(",", "")
        df['漲跌'] = df['漲跌'].str.replace("+", "")
        for i, title in enumerate(df['代號']):
            if (utils.is_number(title) is False or
                (len(title) > 4) or
                    (df['收盤'][i].strip() == "---")):
                df = df.drop([i])
            elif math.isnan(float(title)) is True:
                df = df.drop([i])
        df['成交股數'] = (pd.to_numeric(
            df['成交股數'],
            downcast='integer')/1000).round(0)
        df['發行股數'] = (pd.to_numeric(
            df['發行股數'],
            downcast='integer')/1000).round(0)
        df['date'] = str(self.run_date)
        df['listed'] = "2"
        df = df.reset_index(drop=True)
        return df

    def save(self, df, *args, **kwargs):
        conn = self.connect()
        curs = conn.cursor()
        item = {
            'date': df["date"],
            'stock_id': df["代號"],
            'stock_name': df["名稱"],
            'count_v': df["成交筆數"],
            'open': df["開盤"],
            'high': df["最高"],
            'low': df["最低"],
            'close': df["收盤"],
            'volume': df["成交股數"]
        }
        ins = self._insert_sql(self.table_name, item)
        for idx in df.index:
            values = [x[idx] for x in item.values()]
            curs.execute(ins, values)
        conn.commit()
        conn.close()
