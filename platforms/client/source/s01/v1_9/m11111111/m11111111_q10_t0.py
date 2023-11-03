from pymongo import MongoClient
import pandas as pd
from pandas.tseries.offsets import DateOffset
from datetime import datetime

client = MongoClient('mongodb', 27017)
db = client.tpch

pipeline = [
    {"$lookup": {
        "from": "orders",
        "localField": "C_CUSTKEY",
        "foreignField": "O_CUSTKEY",
        "as": "orders"
    }},
    {"$unwind": "$orders"},
    {"$lookup": {
        "from": "lineitem",
        "localField": "orders.O_ORDERKEY",
        "foreignField": "L_ORDERKEY",
        "as": "orders.lineitems"
    }},
    {"$unwind": "$orders.lineitems"},
    {"$lookup": {
        "from": "nation",
        "localField": "C_NATIONKEY",
        "foreignField": "N_NATIONKEY",
        "as": "nation"
    }},
    {"$unwind": "$nation"},
    {"$match": {
        "orders.O_ORDERDATE": {"$gte": datetime(1993, 10, 1), "$lt": datetime(1994, 1, 1)},
        "orders.lineitems.L_RETURNFLAG": "R"
    }},
    {"$group": {
        "_id": {
            "C_CUSTKEY": "$C_CUSTKEY",
            "C_NAME": "$C_NAME",
            "C_ACCTBAL": "$C_ACCTBAL",
            "C_PHONE": "$C_PHONE",
            "N_NAME": "$nation.N_NAME",
            "C_ADDRESS": "$C_ADDRESS",
            "C_COMMENT": "$C_COMMENT"
        },
        "REVENUE": {"$sum": {"$multiply": ["$orders.lineitems.L_EXTENDEDPRICE", {"$subtract": [1, "$orders.lineitems.L_DISCOUNT"]}]}},
    }},
    {"$sort": {"REVENUE": -1, "_id.C_CUSTKEY": 1, "_id.C_NAME": 1, "_id.C_ACCTBAL": -1}}
]

cursor = db.customer.aggregate(pipeline)
data = list(cursor)
df = pd.json_normalize(data)
df.to_csv('query_output.csv', index=False)
