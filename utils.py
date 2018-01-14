import json
import pandas as pd
import tushare as ts

from bson.json_util import dumps
from datetime import date as Date
from datetime import timedelta as Period
from pymongo import MongoClient

username = ""
password = ""

CAL = ts.trade_cal().set_index('calendarDate')


class Mongo:

    @staticmethod
    def get_mongo_client():
        return MongoClient('mongodb://mongo1,mongo2,mongo3')

    @staticmethod
    def convert_cursor_to_dataframe(col):
        json_data = dumps(col)
        data = json.loads(json_data)
        if len(data) == 0:
            return pd.DataFrame()

        data = data[0]['record']
        data = pd.DataFrame(data)
        return data


class Stock:

    @staticmethod
    def get_all_stock_id():
        stock_info = ts.get_stock_basics()
        stocks = []
        for i in stock_info.index:
            stocks.append(i)
        return stocks

    @staticmethod
    def is_trade_day(date):
        return CAL.loc[date.isoformat()].isOpen == 1

