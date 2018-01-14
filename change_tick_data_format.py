import json
import pandas as pd
import tushare as ts

from bson.json_util import dumps
from datetime import date as Date
from datetime import timedelta as Period
from utils import Mongo as mongo_utils
from utils import Stock as stock_utils

client =  mongo_utils.get_mongo_client()

orig_db = client.tickdata
db = client.stock

start_date = Date(2018,1,13)
end_date = Date(2016,1,1)

TICK_COLLECTION = "tickdata"

PAUSE = 10
RETRY = 10000

for col in orig_db.collection_names():
    if '_' in col:
        code = col.split('_')[0]
        date = col.split('_')[1]
        print "start processing, code: " + code + ", date: " + date
        date = Date(int(date[0:4]), int(date[4:6]), int(date[6:8]))
        if date < start_date and date > end_date:
            cursor = orig_db[col].find({})
            json_data = dumps(cursor)
            data = pd.read_json(json_data).drop('_id', axis = 1)
            dict_data = json.loads(data.to_json(orient='records'))
            print type(dict_data)
            dict_data = { 'code': code, 'date': date.isoformat(), 'record': dict_data }
            #db[TICK_COLLECTION].insert(dict_data)
            print "record is converted!"

