#  Backtest Project

<!--more-->
We want to build a stock screening and back-testing which has the following functions:

    1. Screen stocks using financial and technical data.
    2. Back-test on selected stocks using technical indices.

For example, suppose we want to pick under-valued stocks from S&P 500 and Russel 2000, and trade those stocks depending on KD signal. Criteria for under-valued stocks is the revenue growth greater than 20% in four consecutive quarters and current price is below 50MA, 100MA and 200MA. After screening we want to back-test on those stocks. Rule is we buy when KD crosses up and sell when KD crosses down.

Please describe the process of building this system, from data gathering, data cleaning to module designing in detail.

----
## crawl data
```
market: S&P500, Russel2000
target: price(daily), financial data(quarterly)
```

database: sqlite(for convenient) or else RMDB


### flowchart
![](https://imgur.com/qR02myd.png)

### 1. design database schema
![](https://imgur.com/5frkjqT.png)

### 2. build the BaseCrawler class.
cronjob\crawler\base.py
```
class BaseCrawler(object):

    def __init__(self):
        self.market = ''
        self.symbol = ''
    def connect(self):
    def request(self, *args, **kwargs):
    def parse(self, *args, **kwargs):
    def save(self, *args, **kwargs):
```
```
__init__: set market and symbol.
request: get HTML source code.
parse: tranfer HTML source code to dataframe format.
save: insert parsed data to database.
```

### 3. inherit BaseCrawler class and set the crawler for sp500 price.

因為爬蟲網站可能會有很多不同的格式

故有時我們必須繼承BaseCrawler去針對不同的網站客製爬蟲

cronjob\crawler\sp500_price.py
```
class Sp500PriceCrawler(BaseCrawler):

    def __init__(self):
        self.market = 'SP500'
        self.symbol = [...]
    def request(self, *args, **kwargs):
        ...
    def parse(self, *args, **kwargs):
        ...
    def save(self, *args, **kwargs):
        ...
```
cronjob\crawler\sp500_revenue.py
```
class Sp500RevenueCrawler(BaseCrawler):

    def __init__(self):
        self.market = 'SP500'
        self.symbol = [...]
    def request(self, *args, **kwargs):
        ...
    def parse(self, *args, **kwargs):
        ...
    def save(self, *args, **kwargs):
        ...
```
### 4. check missing data and make sure crawler data is completed.

cronjob\test\base.py
```
class BaseTest(object):

    def connect(self):
    def check_lastdate(self):
        s = 'select max(date) from xxx'
        last_date = pd.read_sql(s, con=self.conn)
        ...
        return str(datetime.date.today()) == last_date
```

```
check_lastdate: check last_date in crawler table is equal to today.
(crawler may fail bacause the website is updated)
```
### 5. run pipeline in the cronjob to crawl data everyday.

cronjob\pipline.py
```
class BasePipeline(object):
    def __int__(self):
        self.crawler = BaseCrawler
        self.schema = BaseSchema
        self.test = BaseTest

    def run(self):
        resp = self.crawler.request()
        df = self.crawler.parse(resp=resp)
        self.crawler.save(df=df)
        ...

class Pipeline:
    def __init__(self):
        self.xxx = xxxPipeline()
        ...

    def run(self):
        for method in self.__dict__.keys():
        ...

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
```

----
## preprocess data for backtesting

### flowchart
![](https://imgur.com/jaZlR30.png)


### 1. Build the BaseData class to get the useful data(price, revenue).

backtest\data\base.py
```
class BaseData():
    def __init__(self, symbol):
        self.conn = ...
        self.symbol = symbol
        table_names = [t[0] for t in list(cursor)]
        self.col2table = {}
        for tname in table_names:
            c = self.conn.execute('PRAGMA table_info(' + tname + ');')
            for cname in [i[1] for i in list(c)]:
                self.col2table[cname] = tname
        self.date = datetime.datetime.now().date()

    def get_price(self):
        s = 'select ...%s' % (self.symbol)
        self.price = pd.read_sql(s, self.conn, index_col = ['date'])
        return self.price

    def get_rev(self):
        s = 'select ...%s' % (self.symbol)
        return pd.read_sql(s, self.conn, index_col = ['date'])

    def get_kd(self):
        kd = abstract.STOCH(self.price)
        return kd
```

```
input stock symbol and get the below data in dataframe format.

price: (index: date, open, high, low, close, volume)
rev: (index: date, revenue)
kd: (index: date, slowk, slowd)
```

### 2. process data and get features(close, 50MA, 100MA, 200MA, KD, revenue_growth_rate).

the initial period of revenue is quarterly.

transfer revenue to growth rate and change period to daily.
```
def to_growth_rate(rev, n):
    growth_rate = (rev.shift(n) - rev.shift(n+1))/rev.shift(n+1)
    ...
    return growth_rate
```
```
price = data.get_price()
close = price['close']
ma_50 = close.rolling(50).mean()
ma_100 = close.rolling(100).mean()
ma_200 = close.rolling(200).mean()
kd = kd = data.get_kd()
rev = data.get_rev()
rev_gr_0 = to_growth_rate(rev, 0)
rev_gr_1 = to_growth_rate(rev, 1)
rev_gr_2 = to_growth_rate(rev, 2)
rev_gr_3 = to_growth_rate(rev, 3)
```
```
price: (index: date, open, high, low, close, volume)
ma_50: mean_average of close (period = 50 days)
kd: kd technical indicators(index: date, slowk, slowd)
rev_gr_0: rev growth rate in the latest quarter
```

### 3. combine features and process missing data.
```
features = pd.concat([...], axis = 1)
features = features.sort_index()
features['...'] = features['...'].fillna(method = 'ffill')
...
```


----
## backtesting

### flowchart
![](https://imgur.com/U09oAqH.jpg)

### 1. Build the BaseStrategy class to preprocess data.

backtest\strategy\base.py

```

Class BaseStrategy():

    def __init__(self):
        self.data = None

    def get_signal(self):
        ...
```
inherit BaseStrategy class, receive all features and write strategy logic to calculate signal.

backtest\strategy\kd_rev.py
```
Class KdRevStrategy(BaseStrategy):

    def __init__(self):
        self.data = None
        self.para1 = 0
        ...

    def get_signal(self):
        ...
        features
        ---preprocess data
        cond1 = (kd.slowk > kd.slowd)
        cond2 = (close<ma_50) & (close<ma_100) & (close<ma_200)
        cond3 = (rev_gr_0>0.2) & (rev_gr_1>0.2) & (rev_gr_2>0.2) & (rev_gr_3>0.2)
        ...
        return signal
```


### 2. Build the BaseSetOption class to set the stock symbol, tax and  range of backtest time.

backtest\set_option\base.py
```
Class BaseSetOption():

    def __init__(self):
        self.initail = None
        self.symbol = None
        self.fee = None
        self.start_date = None
        self.end_date = None
```
backtest\set_option\Sp500.py
```
Class Sp500SetOption(BaseSetOption):

    def __init__(self):
        ...
```
### 3. run backtest and get equity curve and else indicator(sharpe ratio, MDD, etc...).

backtest\backtest_project.py
```
Class BaseBacktest(object):

    def __init__(self):
        self.data = BaseData
        self.strategy = BaseStrategy
        self.setoption = BaseSetOption

    def run(self):
        price = self.data.get_price()
        ...
        self.equity = ...
        self.sharpe_ratio = ...
        self.mdd = ...

    def plot_eq(self):
        ...


class Sp500Backtest(BaseBacktest):

    def __init__(self):
        self.data = Sp500Data()
        self.strategy = KdRevStrategy()
        self.setoption = Sp500SetOption()


if __name__ == '__main__':
    backtest = Sp500Backtest()
    backtest.run()
```