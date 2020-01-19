#  Backtest Project

<!--more-->
We want to build a stock screening and back-testing which has the following functions:

    1. Screen stocks using financial and technical data.
    2. Back-test on selected stocks using technical indices.

For example, suppose we want to pick under-valued stocks from S&P 500 and Russel 2000, and trade those stocks depending on KD signal. Criteria for under-valued stocks is the revenue growth greater than 20% in four consecutive quarters and current price is below 50MA, 100MA and 200MA. After screening we want to back-test on those stocks. Rule is we buy when KD crosses up and sell when KD crosses down.

Please describe the process of building this system, from data gathering, data cleaning to module designing in detail.

----
## 架構圖


確保您具有以下內容：


----
## crawl data

    market: S&P500, Russel2000.
    target: price(daily), financial data(seasonal).

database: sqlite(for convenient) or else RMDB


1. design database schema.

    圖

2. design pattern.

    圖

3. write crawler code.

4. check data leakage and makesure data is complete.

5. run pipeline to crawl data everyday. 


---
    example: Taiwan Stock Exchange(TWSE)


----
## clean data for backtesting

1. find useful data(price, rev) in our case.

2. fufill leakage data.

3. check backtesting period(season to daily).

4. calculate our feature(50MA, 100MA, 200MA, close, KD, rev) 

5. turn sqlite to HDF5(because HDF5 is faster than database)

----
## portfolio backtesting

1. import data(HDF5)

2. set the tax and
get HDF5 data and backtest.

3. backtest daily.

4. it's ideal case but ...

tax & 手續費
選擇回測週期 基本點數多少


----
## display result

powerBI

linebot