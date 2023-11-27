import pymongo
import pandas as pd
from pymongo import MongoClient


def exec_query():
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    lineitem = db.lineitem
    pipeline = [
        {"$match": {"L_SHIPDATE": {"$lte": '1998-09-02'}}},
        {"$group": {
            "_id": {"L_RETURNFLAG": "$L_RETURNFLAG", "L_LINESTATUS": "$L_LINESTATUS"},
            "SUM_QTY": {"$sum": "$L_QUANTITY"},
            "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
            "SUM_DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}}},
            "SUM_CHARGE": {"$sum": {
                "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}, {"$add": [1, "$L_TAX"]}]}}},
            "AVG_QTY": {"$avg": "$L_QUANTITY"},
            "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
            "AVG_DISC": {"$avg": "$L_DISCOUNT"},
            "COUNT_ORDER": {"$sum": 1}}},
    ]
    agg_result = list(lineitem.aggregate(pipeline))
    df = pd.json_normalize(agg_result)
    df.to_csv('query_output.csv')  # export to csv
    client.close()


if __name__ == "__main__":
    exec_query()
