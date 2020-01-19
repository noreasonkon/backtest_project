from crawler.base import BaseSchema


class DataDailyfutureSchema(BaseSchema):
    def __init__(self):
        self.table = 'data_dailyfuture'
        self.schema = {
            "date": "date",
            "future_sname": "string",
            "contract_month": "string",
            "open": "float",
            "high": "float",
            "low": "float",
            "close": "float",
            "volume": "float",
            "end": "float",
            "oi": "float",
            "apm": "string"
        }


class DataDailyoptionSchema(BaseSchema):
    def __init__(self):
        self.table = 'data_dailyoption'
        self.schema = {
            "date": "date",
            "future_sname": "string",
            "contract_month": "string",
            "open": "float",
            "high": "float",
            "low": "float",
            "close": "float",
            "volume": "float",
            "end": "float",
            "oi": "float",
            "apm": "string"
        }


class DataYahooSchema(BaseSchema):
    def __init__(self):
        self.table = 'data_yahoo'
        self.schema = {
            'date': 'date',
            'commodity': 'string',
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float',
            'adj': 'float',
            'volume': 'float'
        }
