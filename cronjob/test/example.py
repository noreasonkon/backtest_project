import pandas as pd
import sqlite3
import os

path = r'D:\Users\walker.yang\Documents\Github\tradeAnalysisServer'
conn = sqlite3.connect(os.path.join(path, 'data.sqlite3'))
connF = sqlite3.connect(os.path.join(path, 'FutureOP.db'))
sql_d = 'select  distinct date from data_dailyfuture order by date desc'
dates = pd.read_sql(sql_d, conn)
table_names = ['data_dailyfuture',
               'data_dailyoption',
               'data_dailystock',
               'data_dailywarrant',
               'data_dailyindex',
               'bigps_dailyfuture',
               'bigps_dailyoption',
               'threeps_dailyfuture',
               'threeps_dailyoption',
               'threeps_dailystock',
               'credit_dailyindex',
               'credit_dailystock',
               'data_minutelytx',
               ]

for table_name in table_names:
    for date in dates.date[1:5]:
        sql = 'select * from %s where date = "%s" ' % (table_name, date)
        data = pd.read_sql(sql, conn)
        sql_f = 'select * from %s where date = "%s"' % (table_name, date)
        data_f = pd.read_sql(sql_f, connF)
    if (not data.equals(data_f)):
        print(table_name, date)
        continue
table_names = ['pmi_monthlytx',
               'allps_weeklystock',
               'profit_monthlystock',
               ]

table_names = ['ft_contractclose',
               'ft_txo_pc_ratio',
               'ft_contractclose',
               #  'ft_txo_max_oi',
               'ft_txo_price',
               #  'ft_txo_df_oi',
               ]
for table_name in table_names:
    sql = ('select * from %s where date >= "2003-01-03" order by date' %
           table_name)
    data = pd.read_sql(sql, conn)
    sql_f = ('select * from %s where date >= "2003-01-03" order by date' %
             table_name)
    data_f = pd.read_sql(sql_f, connF)
    if (not data.equals(data_f)):
        print(table_name)


table_names = ['dt_future_info'
               ]
for table_name in table_names:
    sql = 'select * from %s ' % table_name
    data = pd.read_sql(sql, conn)
    sql_f = 'select * from %s' % table_name
    data_f = pd.read_sql(sql_f, connF)
    if (not data.equals(data_f)):
        print(table_name)

table_names = ['ft_contractdate',
               ]
for table_name in table_names:
    sql = 'select * from %s order by contract' % table_name
    data = pd.read_sql(sql, conn)
    sql_f = 'select * from %s order by contract' % table_name
    data_f = pd.read_sql(sql_f, connF)
    if (not data.equals(data_f)):
        print(table_name)
