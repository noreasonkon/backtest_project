# coding=utf-8
import datetime
import calendar


def fmt_float(value):
    try:
        return float(value)
    except:
        return 0.0


def get_period(year, month):
    """
    Get first and last day of specified year and month
    """
    first_weekday, days = calendar.monthrange(year, month)
    first = datetime.date(year=year, month=month, day=1)
    last = datetime.date(year=year, month=month, day=days)
    return first, last


def to_ROCdate(date):
    return datetime.date(
        date.year-1911, date.month,
        date.day).strftime('%Y/%m/%d')[1:]


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False


def get_today(dtype='str', fmt='%Y-%m-%d'):
    today = datetime.datetime.today()
    if dtype == 'str':
        return today.strftime(fmt)
    elif dtype == 'datetime':
        return today
    else:
        raise 'Invalid dtype: %s' % dtype
