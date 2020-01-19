# coding=utf-8
import requests
import datetime
from io import StringIO
import pandas as pd
from crawler.base import BaseCrawler
from common import utils
import math
import time


class ProfitMonthlyIPOStockCrawler(BaseCrawler):
    def __init__(self):
        self.table_name = 'profit_monthlystock'
        self.start_date = datetime.date.today()
        self.run_date = self.start_date
        self.end_date = self._get_last_date(table=self.table_name)

    def request(self, *args, **kwargs):
        year = int(str(self.run_date).split("-")[0])
        month = int(str(self.run_date).split("-")[1])
        self.year_month = str(year) + "-" + str(month).zfill(2)
        if month == 12:
            self.this_date = str(year+1) + "-01-10"
        else:
            self.this_date = str(year) + "-" + str(month+1).zfill(2) + "-10"
        roc_year = year - 1911
        if roc_year <= 98:
            url = ('https://mops.twse.com.tw/nas/t21/sii/t21' +
                   'sc03_{0}_{1}.html').format(str(roc_year), str(month))
        else:
            url = ('https://mops.twse.com.tw/nas/t21/sii/t21' +
                   'sc03_{0}_{1}_0.html').format(str(roc_year), str(month))
        headers = {'User-Agent':
                   ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1)' +
                    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0' +
                    '.2171.95 Safari/537.36')}
        print(url)
        return requests.get(url, headers)

    def parse(self, resp, *args, **kwargs):
        resp.encoding = 'big5'
        html_df = pd.read_html(StringIO(resp.text))
        if html_df[0].shape[0] > 500:
            df = html_df[0].copy()
        else:
            df = pd.concat([df for df in html_df
                            if df.shape[1] > 2])
        df.columns = [x[1] for x in df.columns]
        df = df.reset_index(drop=True)
        for i, title in enumerate(df['公司代號']):
            if utils.is_number(title) is False:
                df = df.drop([i])
            elif math.isnan(float(title)) is True:
                df = df.drop([i])
        df = df.reset_index(drop=True)
        df['listed'] = 1
        df = df.astype(str)
        df['date'] = str(self.this_date)
        df['month'] = self.year_month
        time.sleep(5)
        return df

    def save(self, df, *args, **kwargs):
        conn = self.connect()
        curs = conn.cursor()
        for idx in df.index:
            data = {
                'month': df["month"][idx],
                'date': df["date"][idx],
                'stock_id': df["公司代號"][idx],
                'stock_name': df["公司名稱"][idx],
                'this_month_profit': df["當月營收"][idx],
                'this_cum_profit': df["當月累計營收"][idx],
                'notes': df["備註"][idx],
                'listed': df["listed"][idx]
            }
            ins = self._insert_cmd(table=self.table_name, columns=data.keys())
            curs.execute(ins, tuple(data.values()))
        conn.commit()
        conn.close()


class ProfitMonthlyOTCStockCrawler(BaseCrawler):
    def __init__(self):
        self.table_name = 'profit_monthlystock'
        self.start_date = datetime.date.today()
        self.run_date = self.start_date
        self.end_date = self._get_last_date(table=self.table_name)

    def request(self, *args, **kwargs):
        year = int(str(self.run_date).split("-")[0])
        month = int(str(self.run_date).split("-")[1])
        self.year_month = str(year) + "-" + str(month).zfill(2)
        if month == 12:
            self.this_date = str(year+1) + "-01-10"
        else:
            self.this_date = str(year) + "-" + str(month+1).zfill(2) + "-10"
        roc_year = year - 1911
        if roc_year <= 98:
            url = ('https://mops.twse.com.tw/nas/t21/otc/t21' +
                   'sc03_{0}_{1}.html').format(str(roc_year), str(month))
        else:
            url = ('https://mops.twse.com.tw/nas/t21/otc/t21' +
                   'sc03_{0}_{1}_0.html').format(str(roc_year), str(month))
        headers = {'User-Agent':
                   ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1)' +
                    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0' +
                    '.2171.95 Safari/537.36')}
        print(url)
        return requests.get(url, headers)

    def parse(self, resp, *args, **kwargs):
        resp.encoding = 'big5'
        html_df = pd.read_html(StringIO(resp.text))
        if html_df[0].shape[0] > 500:
            df = html_df[0].copy()
        else:
            df = pd.concat([df for df in html_df if df.shape[1] > 2])
        df.columns = [x[1] for x in df.columns]
        df = df.reset_index(drop=True)
        for i, title in enumerate(df['公司代號']):
            if utils.is_number(title) is False:
                df = df.drop([i])
            elif math.isnan(float(title)) is True:
                df = df.drop([i])
        df = df.reset_index(drop=True)
        df['listed'] = 2
        df = df.astype(str)
        df['date'] = str(self.this_date)
        df['month'] = self.year_month
        time.sleep(5)
        return df

    def save(self, df, *args, **kwargs):
        conn = self.connect()
        curs = conn.cursor()
        for idx in df.index:
            data = {
                'month': df["month"][idx],
                'date': df["date"][idx],
                'stock_id': df["公司代號"][idx],
                'stock_name': df["公司名稱"][idx],
                'this_month_profit': df["當月營收"][idx],
                'this_cum_profit': df["當月累計營收"][idx],
                'notes': df["備註"][idx],
                'listed': df["listed"][idx]
            }
            ins = self._insert_cmd(table=self.table_name, columns=data.keys())
            curs.execute(ins, tuple(data.values()))
        conn.commit()
        conn.close()
