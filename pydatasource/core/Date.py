import datetime
import os
from dateutil.relativedelta import relativedelta


def set_now():
    date = None
    if os.environ.get('STATIC_SERVER'):
        date = os.environ.get('DATE_NOW')
    if not date:
        date = str(datetime.datetime.now())[:10]
    return datetime.datetime.strptime(date, '%Y-%m-%d') if date else datetime.datetime.now()


def get_last_year(day):
    try:
        return datetime.datetime(day.year - 1, day.month, day.day)
    except:  # In case of February 29th -> We took February 28th of the last year
        return datetime.datetime(day.year - 1, day.month, day.day - 1)


def get_quarter(date):
    return int((date.month - 1) / 3 + 1)


def get_first_day_of_the_quarter(date):
    quarter = get_quarter(date)
    return datetime.datetime(date.year, 3 * quarter - 2, 1)


def get_first_day_of_the_month(date):
    return datetime.datetime(date.year, date.month, 1)


def get_first_day_of_the_year(date):
    return datetime.datetime(date.year, 1, 1)


class Date:
    @staticmethod
    def day_before():
        return set_now() + datetime.timedelta(days=-2)

    # day_before = set_now() + datetime.timedelta(days=-2)
    @staticmethod
    def yesterday():
        return set_now() + datetime.timedelta(days=-1)

    @classmethod
    def yesterday_string_day_of_week(cls):
        return cls.yesterday().strftime('%A')

    @staticmethod
    def yesterday_first_day_last_current_6_months():
        return set_now() + relativedelta(months=-6)

    @staticmethod
    def yesterday_first_day_last_previous_6_months():
        return set_now() + relativedelta(months=-12)

    @staticmethod
    def yesterday_last_previous_6_months():
        return set_now() + relativedelta(months=-6, days=-1)

    @staticmethod
    def yesterday_last_week():
        return set_now() + datetime.timedelta(days=-8)

    @staticmethod
    def yesterday_l7d_stop():
        return set_now() + datetime.timedelta(days=-1)

    @staticmethod
    def yesterday_l7d_start():
        return set_now() + datetime.timedelta(days=-7)

    @staticmethod
    def yesterday_previous_l7d_stop():
        return set_now() + datetime.timedelta(days=-8)

    @staticmethod
    def yesterday_previous_l7d_start():
        return set_now() + datetime.timedelta(days=-14)

    @staticmethod
    def yesterday_l30d_stop():
        return set_now() + datetime.timedelta(days=-1)

    @staticmethod
    def yesterday_l30d_start():
        return set_now() + datetime.timedelta(days=-30)

    @staticmethod
    def yesterday_previous_l30d_stop():
        return set_now() + datetime.timedelta(days=-31)

    @staticmethod
    def yesterday_previous_l30d_start():
        return set_now() + datetime.timedelta(days=-60)

    @staticmethod
    def yesterday_l45d():
        return set_now() + datetime.timedelta(days=-45)

    @classmethod
    def yesterday_l7d_last_year(cls):
        return get_last_year(cls.yesterday()) + datetime.timedelta(days=-7)

    @classmethod
    def yesterday_week(cls):
        return cls.yesterday() + datetime.timedelta(days=-cls.yesterday().weekday())

    @classmethod
    def yesterday_first_day_last_week(cls):
        return cls.yesterday_last_week() + datetime.timedelta(days=-cls.yesterday_last_week().weekday())

    @classmethod
    def yesterday_last_year(cls):
        return get_last_year(cls.yesterday())

    @classmethod
    def yesterday_first_day_quarter(cls):
        return get_first_day_of_the_quarter(cls.yesterday())

    @classmethod
    def yesterday_first_day_month(cls):
        return get_first_day_of_the_month(cls.yesterday())

    @classmethod
    def yesterday_first_day_year(cls):
        return get_first_day_of_the_year(cls.yesterday())

    @classmethod
    def yesterday_first_day_last_year(cls):
        return get_first_day_of_the_year(cls.yesterday_last_year())

    @classmethod
    def yesterday_first_day_last_month(cls):
        return get_first_day_of_the_month(cls.yesterday_last_month())

    @classmethod
    def yesterday_first_day_last_month_yyyymmdd(cls):
        return cls.yesterday_first_day_last_month().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_last_year_last_day_of_month(cls):
        return datetime.datetime(
            year=cls.yesterday_last_year().year,
            month=cls.yesterday_last_year().month,
            day=1) + relativedelta(day=1, months=+1, days=-1)

    @classmethod
    def yesterday_last_day_of_month(cls):
        return datetime.datetime(
            year=cls.yesterday().year,
            month=cls.yesterday().month,
            day=1) + relativedelta(day=1, months=+1, days=-1)

    @classmethod
    def yesterday_last_day_of_last_month(cls):
        return datetime.datetime(
            year=cls.yesterday().year,
            month=cls.yesterday().month,
            day=1) + relativedelta(days=-1)

    @classmethod
    def yesterday_last_day_of_last_month_yyyymmdd(cls):
        return cls.yesterday_last_day_of_last_month().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_last_day_of_year(cls):
        return datetime.datetime(
            year=cls.yesterday().year,
            month=12,
            day=31)

    @staticmethod
    def last45_yyyymmdd():
        return (set_now() + datetime.timedelta(days=-45)).strftime('%Y-%m-%d')

    @classmethod
    def day_before_yyyymmdd(cls):
        return cls.day_before().strftime('%Y-%m-%d')

    @classmethod
    def day_before_last_year_yyyymmdd(cls):
        day_before = cls.day_before()
        if day_before.day == 29 and day_before.month == 2:
            day_before = set_now() + datetime.timedelta(days=-3)
        return datetime.datetime(day_before.year - 1, day_before.month, day_before.day).strftime(
            '%Y-%m-%d')

    @classmethod
    def yesterday_yyyymmdd(cls):
        return cls.yesterday().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_first_day_month_yyyymmdd(cls):
        return cls.yesterday_first_day_month().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_last_week_yyyymmdd(cls):
        return cls.yesterday_last_week().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_last_month(cls):
        return cls.yesterday() + relativedelta(months=-1)

    @classmethod
    def yesterday_last_month_yyyymmdd(cls):
        return cls.yesterday_last_month().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_last_quarter(cls):
        return cls.yesterday() + relativedelta(months=-3)

    @classmethod
    def yesterday_last_quarter_yyyymmdd(cls):
        return cls.yesterday_last_quarter().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_previous_l7d_start_yyyymmdd(cls):
        return cls.yesterday_previous_l7d_start().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_previous_l7d_stop_yyyymmdd(cls):
        return cls.yesterday_previous_l7d_stop().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_l7d_start_yyyymmdd(cls):
        return cls.yesterday_l7d_start().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_week_yyyymmdd(cls):
        return cls.yesterday_week().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_first_day_last_week_yyyymmdd(cls):
        return cls.yesterday_first_day_last_week().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_l7d_stop_yyyymmdd(cls):
        return cls.yesterday_l7d_stop().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_first_day_last_quarter(cls):
        date = cls.yesterday() + relativedelta(months=-3)
        date = get_first_day_of_the_quarter(date)
        return date

    @classmethod
    def yesterday_first_day_last_quarter_yyyymmdd(cls):
        return cls.yesterday_first_day_last_quarter().strftime('%Y-%m-%d')

    @classmethod
    def yesterday_last_month_end_of_month(cls):
        return datetime.datetime(cls.yesterday().year, cls.yesterday().month, 1) + datetime.timedelta(days=-1)

    @classmethod
    def yesterday_last_month_end_of_month_yyyymmdd(cls):
        return cls.yesterday_last_month_end_of_month().strftime('%Y-%m-%d')
