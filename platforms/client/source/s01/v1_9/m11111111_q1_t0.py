from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta

def connect_mongodb():
    client = MongoClient("mongodb://mongodb:27017/")
    db = client['tpch']
    return db

def get_data(db):
    pipeline = [
        {
            "$match":  {
                'L_SHIPDATE': {'$lte': datetime.strptime('1998-09-02', "%Y-%m-%d")}
            }
        },
        {
            "$group": {
                "_id": {'L_RETURNFLAG': "$L_RETURNFLAG",
                        'L_LINESTATUS': "$L_LINESTATUS"},
                'SUM_QTY': {'$sum': "$L_QUANTITY"},
                'SUM_BASE_PRICE': {'$sum': "$L_EXTENDEDPRICE"},
                'SUM_DISC_PRICE': {'$sum': {'$multiply': ["$L_EXTENDEDPRICE", {'$subtract': [1, "$L_DISCOUNT"]}] }},
                'SUM_CHARGE': {'$sum': {'$multiply': ["$L_EXTENDEDPRICE", {'$subtract': [1, "$L_DISCOUNT"]}, {'$add': [1, "$L_TAX"]}] }},
                'AVG_QTY': {'$avg': "$L_QUANTITY"},
                'AVG_PRICE': {'$avg': "$L_EXTENDEDPRICE"},
                'AVG_DISC': {'$avg': "$L_DISCOUNT"},
                'COUNT_ORDER': {'$sum': 1}
            }
        },
        {
            "$sort": {'_id': 1}
        }
    ]

    collect = db.lineitem.aggregate(pipeline)
    return collect

def write_to_csv(collect):
    df = pd.DataFrame(list(collect))
    df.to_csv('query_output.csv', index=False)

if __name__ == "__main__":
    db = connect_mongodb()
    collect = get_data(db)
    write_to_csv(collect)
