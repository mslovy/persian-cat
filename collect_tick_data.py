import json
import tushare as ts

from datetime import date as Date
from datetime import timedelta as Period
from utils import Mongo as mongo_utils
from utils import Stock as stock_utils

client =  mongo_utils.get_mongo_client()

db = client.stock
col = db.tickdata

start_date = Date(2018,1,12)
end_date = Date(2017,10,1)

PAUSE = 10
RETRY = 10000

stock_list = stock_utils.get_all_stock_id()
count = len(stock_list)
for code in stock_list:
    print "There is " + str(count) + " left to processing"
    count = count - 1
    date = start_date
    while True:
        if not stock_utils.is_trade_day(date):
            date = date - Period(1)
            print "skip date: " + date.isoformat() + ", as it is not trade date"
            continue

        if date < end_date:
            print "date is < end_date, stop!"
            break

        tick_record = col.find({'code': code, 'date': date.isoformat()})

        if tick_record.count() != 0:
            date = date - Period(1)
            print "skip date: " + date.isoformat() + ", code: " + code + " is in the database"
            continue

        print "query tick data for code: " + code + ", date: " + date.isoformat()
        tick_data = ts.get_tick_data(code, date = date.isoformat(), pause = PAUSE, retry_count = RETRY)
        print "persistent data for code: " + code + ", date: " + date.isoformat()
        tick_data = {'code': code, 'date': date.isoformat(), 'record': json.loads(tick_data.to_json(orient='records'))}
        col.insert(tick_data)
        date = date - Period(1)
