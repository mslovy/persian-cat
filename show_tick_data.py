import json
import tushare as ts

from datetime import date as Date
from datetime import timedelta as Period
from utils import Mongo as mongo_utils
from utils import Stock as stock_utils

client =  mongo_utils.get_mongo_client()

db = client.stock

start_date = Date(2018,1,12)
end_date = Date(2017,12,1)

PAUSE = 10
RETRY = 10000


cursor = db.tickdata.find({'code': '002117', 'date': '2017-11-02'})

data = mongo_utils.convert_cursor_to_dataframe(cursor)

print data
