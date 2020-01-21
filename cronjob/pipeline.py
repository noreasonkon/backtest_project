import datetime
import time
from crawler.base import BaseCrawler
from crawler.base import BaseSchema

pool = ThreadPoolExecutor(1)
INITIAL_REF_NUMBER = 1


class BasePipeline(object):
    def __int__(self):
        self.crawler = BaseCrawler
        self.schema = BaseSchema
        self.last_number = 1

    @CrawlerLog()
    def run(self):
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


class Sp500PricePipeline(BasePipeline):
    def __init__(self):
        self.crawler = data_dailystock.DataDailyIPOstockCrawler()
        self.crawler.run_date = datetime.date.today() + \
            datetime.timedelta(days=-INITIAL_REF_NUMBER)
        self.last_number = 1

class Pipeline:
    def __init__(self):
        self.sp500_price = Sp500PricePipeline()
        ...

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
