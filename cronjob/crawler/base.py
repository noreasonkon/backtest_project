import datetime
import sqlite3
import pandas as pd
from settings import DB_SETTING


class BaseCrawler(object):

    def __init__(self):
        self.commodity_list = []
        self.commodity = ''

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

    def _get_last_date(self, table: str, commodity=''):
        conn = self.connect()
        constraint_cmd = "WHERE `commodity`='%s'" % commodity \
            if commodity else ''
        sql_cmd = "SELECT MAX(`date`) FROM `%s` %s LIMIT 1" % (
            table, constraint_cmd)
        df = pd.read_sql(sql=sql_cmd, con=conn)
        return datetime.datetime.strptime(df.values[0][0], '%Y-%m-%d').date()

    @staticmethod
    def _insert_cmd(table: str, columns: list):
        cols = ', '.join(['`%s`' % k for k in columns])
        num = ','.join('?' * len(columns))
        ins = 'insert or replace into %s(%s) values(%s)' % (table, cols, num)
        return ins

    @staticmethod
    def _insert_sql(table: str, item: dict):
        ins = """insert or replace into %s(
            %s) values(%s)""" % (table,
                                 ', '.join(item.keys()),
                                 ', '.join(['?'] * len(item.keys())))
        return ins


class SchemaError(Exception):
    def __init__(self, field, fmt):
        self.field = field
        self.fmt = fmt

    def __str__(self):
        return 'SchemaError: Field %s is not %s' % (self.field, self.fmt)


class BaseSchema(object):
    db_setting = DB_SETTING

    def __init__(self):
        self.table = ''
        self.schema = {}

    def connect(self):
        if self.db_setting['DB_TYPE'] == 'sqlite':
            return sqlite3.connect(self.db_setting['NAME'],
                                   check_same_thread=False)
        else:
            raise NotImplementedError()

    def validate(self, data, *args, **kwargs):
        for k, v in data.items():
            if v == 'string' and not isinstance(v, str):
                raise SchemaError(k, v)
            elif v == 'float' and not isinstance(v, float):
                raise SchemaError(k, v)
            elif v == 'integer' and not isinstance(v, int):
                raise SchemaError(k, v)
            elif v == 'datetime' and not isinstance(v, datetime.datetime):
                raise SchemaError(k, v)
            elif v == 'date' and not isinstance(v, datetime.date):
                raise SchemaError(k, v)
        return True

    def mapping(self, schema, *args, **kwargs):
        table = {
            "string": {
                "sqlite": "CHAR",
                "mysql": "VARCHAR"
            },
            "integer": {
                "sqlite": "INTEGER",
                "mysql": "INTEGER"
            },
            "float": {
                "sqlite": "REAL",
                "mysql": "FLOAT"
            },
            "date": {
                "sqlite": "DATE",
                "mysql": "DATE"
            },
            "datetime": {
                "sqlite": "DATETIME",
                "mysql": "DATETIME"
            }
        }
        for k, v in schema.items():
            schema[k] = table[v][self.db_setting['DB_TYPE']]
        return schema

    def create(self, *args, **kwargs):
        columns = ', '.join("{!s} {!s}".format(k, v)
                            for k, v in self.mapping(self.schema).items())
        sql = 'CREATE TABLE IF NOT EXISTS `%s` (%s)' % (self.table, columns)
        with self.connect() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
            except Exception as e:
                raise Exception('fail to create table %s: %s' % (
                    self.table, e))
