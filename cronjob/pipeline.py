import datetime
from concurrent.futures import ThreadPoolExecutor
from crawler import data_dailyfuture, data_dailyoption
from crawler import bigps_dailyfuture, bigps_dailyoption
from crawler import threeps_dailyfuture, threeps_dailyoption
from crawler import data_dailystock, threeps_dailystock
from crawler import data_dailywarrant, allps_weeklystock
from crawler import pmi_monthlytx, credit_dailyindex
from crawler import credit_dailystock, data_minutelytx
from crawler import profit_monthlystock, ft_stock_amount
from crawler import data_dailyindex, dt_future_info
from crawler import data_yahoo
from crawler.schema import DataDailyfutureSchema, DataDailyoptionSchema
from crawler.schema import DataYahooSchema
from process import contract_close
from process import ft_data_dailyoption
import time
from crawler.base import BaseCrawler
from crawler.base import BaseSchema
from common.decorators import CrawlerLog
import pdb

pool = ThreadPoolExecutor(1)
INITIAL_REF_NUMBER = 1


class BasePipeline(object):
    def __int__(self):
        self.crawler = BaseCrawler
        self.schema = BaseSchema
        self.last_number = 1

    @CrawlerLog()
    def run(self):
        # self.schema.create()
        while True:
            print('start')
            pdb.set_trace()
            resp = self.crawler.request()
            print('> running %s' % self.crawler.run_date)
            df = self.crawler.parse(resp=resp)
            print(df)
            self.crawler.save(df=df)
            if self.crawler.run_date <= self.crawler.end_date:
                break
            else:
                time.sleep(5)
                self.crawler.run_date += datetime.timedelta(
                    days=-self.last_number)


class MultiPipeline(object):
    def __int__(self):
        self.crawler = BaseCrawler
        self.schema = BaseSchema
        self.last_number = 1

    def _job(self, commodity):
        self.crawler.commodity = commodity
        resp = self.crawler.request()
        df = self.crawler.parse(resp=resp)
        self.crawler.save(df=df)

    @CrawlerLog()
    def run(self):
        while True:
            for commodity in self.crawler.commodity_list:
                self._job(commodity=commodity)
            if self.crawler.run_date <= self.crawler.end_date:
                break
            else:
                time.sleep(5)
                self.crawler.run_date += datetime.timedelta(
                    days=-self.last_number)


class DataDailyfuturePipeline(BasePipeline):
    def __init__(self):
        self.crawler = data_dailyfuture.DataDailyfutureCrawler()
        self.schema = DataDailyfutureSchema()
        self.last_number = 1


class DataDailyoptionPipeline(BasePipeline):
    def __init__(self):
        self.crawler = data_dailyoption.DataDailyoptionCrawler()
        self.schema = DataDailyfutureSchema()
        self.last_number = 1


class BigpsDailyfuturePipeline(BasePipeline):
    def __init__(self):
        self.crawler = bigps_dailyfuture.BigpsDailyfutureCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 30


class BigpsDailyoptionPipeline(BasePipeline):
    def __init__(self):
        self.crawler = bigps_dailyoption.BigpsDailyoptionCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 30


class ThreepsDailyFuturePipeline(MultiPipeline):
    def __init__(self):
        self.crawler = threeps_dailyfuture.ThreepsDailyFutureCrawler()
        self.crawler.commodity_list = ['MXF', 'TXF']
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 30


class ThreepsDailyOptionPipeline(MultiPipeline):
    def __init__(self):
        self.crawler = threeps_dailyoption.ThreepsDailyOptionCrawler()
        self.crawler.commodity_list = ['TXO']
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 30


class DataDailyIPOstockPipeline(BasePipeline):
    def __init__(self):
        self.crawler = data_dailystock.DataDailyIPOstockCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 1


class ThreepsDailyIPOstockPipeline(BasePipeline):
    def __init__(self):
        self.crawler = threeps_dailystock.ThreepsDailyIPOstockCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 1


class ThreepsDailyOTCstockPipeline(BasePipeline):
    def __init__(self):
        self.crawler = threeps_dailystock.ThreepsDailyOTCstockCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 1


class DataDailyOTCstockPipeline(BasePipeline):
    def __init__(self):
        self.crawler = data_dailystock.DataDailyOTCstockCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 1


class DataDailyIPOWarrantPipeline(MultiPipeline):
    def __init__(self):
        self.crawler = data_dailywarrant.DataDailyIPOWarrantCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.crawler.commodity_list = ['0999', '0999P']
        self.last_number = 1


class Allps_WeeklystockPipeline:
    def __int__(self):
        self.schema = None
        self.last_number = 1

    @CrawlerLog()
    def run(self):
        self.crawler = allps_weeklystock.Allps_WeeklystockCrawler()
        resp = self.crawler.request()
        df = self.crawler.parse(resp=resp)
        if self.crawler.download_date != str(self.crawler.last_date):
            self.crawler.temp_save()
            df = self.crawler.get_df(df)
            self.crawler.save(df=df)


class PmiMonthlytxPipeline:
    def __int__(self):
        self.schema = None
        self.last_number = None

    @CrawlerLog()
    def run(self):
        self.crawler = pmi_monthlytx.PmiMonthlytxCrawler()
        resp = self.crawler.request()
        df = self.crawler.parse(resp=resp)
        if df['month'][df.index.max()] != self.crawler.end_month:
            self.crawler.save(df=df)


class CreditDailyindexPipline(BasePipeline):
    def __init__(self):
        self.crawler = credit_dailyindex.CreditDailyindexCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 1


class CreditDailyIPOstockPipline(BasePipeline):
    def __init__(self):
        self.crawler = credit_dailystock.CreditDailyIPOstockCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 1


class CreditDailyOTCstockPipline(BasePipeline):
    def __init__(self):
        self.crawler = credit_dailystock.CreditDailyOTCstockCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 1


class DataMinutelytxPipline(BasePipeline):
    def __init__(self):
        self.crawler = data_minutelytx.DataMinutelytxCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 1


class DataDailyindexPipline(MultiPipeline):
    def __init__(self):
        self.crawler = data_dailyindex.DataDailyindexCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-0)
        self.crawler.end_date = datetime.date.today() + \
            datetime.timedelta(days=-0)
        self.crawler.commodity_list = ["XX-^TNX", "NYSE-TSM",
                                       "INDEXNASDAQ-^SOX", "KRX-^KOSPI",
                                       "INDEXDJX-^DJI", "NASDAQ-CATY",
                                       "INDEXHANGSENG-^HSI", "XX-^SPY",
                                       "XX-XLU"]


class DataDailySTWindexPipline(MultiPipeline):
    def __init__(self):
        self.crawler = data_dailyindex.DataDailySTWindexCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-0)
        self.crawler.end_date = datetime.date.today() + \
            datetime.timedelta(days=-0)
        self.crawler.commodity_list = ["MTIF"]


class ContractClosePipeline:
    def __init__(self):
        self.process = contract_close.ContractCloseProcess()

    @CrawlerLog()
    def run(self):
        df_raw = self.process.select()
        df_parsed = self.process.parse(df_raw)
        self.process.save(df_parsed)


class FtContractclosePipeline():
    def __init__(self):
        self.process = ft_data_dailyoption.FtContractcloseProcess()

    @CrawlerLog()
    def run(self):
        df = self.process.contractclose()
        print(df)
        self.process.save(df)


class FtContractdatePipeline:
    def __init__(self):
        self.process = ft_data_dailyoption.FtContractdateProcess()

    @CrawlerLog()
    def run(self):
        df = self.process.select()
        self.process.save(df)


class FtTxoPcRatioPipeline:
    def __init__(self):
        self.process = ft_data_dailyoption.FtTxoPcRatioProcess()

    @CrawlerLog()
    def run(self):
        df = self.process.select()
        print(df)
        self.process.save(df)


class FtTxoMaxOiPipeline:
    def __init__(self):
        self.process = ft_data_dailyoption.FtTxoMaxOiProcess()

    @CrawlerLog()
    def run(self):
        df_month = self.process.max_oi_month(filt_close=15)
        df_week = self.process.max_oi_week(filt_close=5)
        df = self.process.merge(df_month, df_week)
        self.process.save(df)


class DtFutureInfoPipeline:
    def __init__(self):
        self.crawler = dt_future_info.DtFutureInfoCrawler()

    def run(self):
        resp = self.crawler.request()
        df = self.crawler.parse(resp=resp)
        self.crawler.save(df=df)


class FtTxoPricePipeline:
    def __init__(self):
        self.process = ft_data_dailyoption.FtTxoPriceProcess()

    def run(self):
        df = self.process.price(filt_close=5)
        # print(df)
        self.process.save(df)


class FtTxoDfOiPipeline:
    def __init__(self):
        self.process = ft_data_dailyoption.FtTxoDfOiProcess()

    def run(self):
        df = self.process.select()
        self.process.update(df)


class ProfitMonthlyIPOStockPipeline:
    def __init__(self):
        self.crawler = profit_monthlystock.ProfitMonthlyIPOStockCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-30)
        self.crawler.end_date = datetime.date.today() + \
            datetime.timedelta(days=-60)

    def run(self):
        resp = self.crawler.request()
        df = self.crawler.parse(resp=resp)
        print(df)
        self.crawler.save(df=df)


class ProfitMonthlyOTCStockPipeline:
    def __init__(self):
        self.crawler = profit_monthlystock.ProfitMonthlyOTCStockCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-30)
        self.crawler.end_date = datetime.date.today() + \
            datetime.timedelta(days=-60)

    def run(self):
        resp = self.crawler.request()
        df = self.crawler.parse(resp=resp)
        print(df)
        self.crawler.save(df=df)


class FtStockAmountPipline(BasePipeline):
    def __init__(self):
        self.crawler = ft_stock_amount.FtStockAmountCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 1


class Pipeline:
    def __init__(self):
        # self.data_dailyfuture = DataDailyfuturePipeline()
        # self.data_dailyoption = DataDailyoptionPipeline()
        # self.bigps_dailyfuture = BigpsDailyfuturePipeline()
        # self.bigps_dailyoption = BigpsDailyoptionPipeline()
        # self.threeps_dailyfuture = ThreepsDailyFuturePipeline()
        # self.threeps_dailyoption = ThreepsDailyOptionPipeline()
        # self.data_dailyIPOstock = DataDailyIPOstockPipeline()
        # self.data_dailyOTCstock = DataDailyOTCstockPipeline()
        # self.threeps_dailyIPOstock = ThreepsDailyIPOstockPipeline()
        # self.threeps_dailyOTCstock = ThreepsDailyOTCstockPipeline()
        # self.data_dailywarrant = DataDailyIPOWarrantPipeline()
        # # self.allps_weeklystock = Allps_WeeklystockPipeline()
        # self.credit_dailyindex = CreditDailyindexPipline()
        # self.credit_dailyIPOstock = CreditDailyIPOstockPipline()
        # self.credit_dailyOTCstock = CreditDailyOTCstockPipline()
        self.data_minutelytx = DataMinutelytxPipline()
        # self.pmi_monthlytx = PmiMonthlytxPipeline()
        # self.data_dailyindex = DataDailyindexPipline()
        # self.data_dailySTWindex = DataDailySTWindexPipline()
        # self.ft_stock_amount = FtStockAmountPipline()

        # self.ft_contractclose = FtContractclosePipeline()
        # self.ft_contractdate = FtContractdatePipeline()
        # self.dt_future_info = DtFutureInfoPipeline()
        # self.ft_txo_max_oi = FtTxoMaxOiPipeline()
        # self.ft_txo_price = FtTxoPricePipeline()
        # self.ft_txo_pc_ratio = FtTxoPcRatioPipeline()
        # self.ft_txo_df_oi = FtTxoDfOiPipeline()
        # self.profit_monthlyIPOstock = ProfitMonthlyIPOStockPipeline()
        # self.profit_monthlyOTCstock = ProfitMonthlyOTCStockPipeline()

    def run(self):
        print('Pipeline is working')
        for method in self.__dict__.keys():
            print('working on %s ...' % method)
            try:
                time.sleep(3)
                getattr(self, method).run()
                print('Pipeline Success %s' % method)
            except Exception as e:
                print('Pipeline Fail %s: %s' % (method, e))


if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
