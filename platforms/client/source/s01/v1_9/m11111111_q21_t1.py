from pymongo import MongoClient
import pandas as pd

client = MongoClient('mongodb://mongodb:27017/')

db = client.tpch
supplier = db.supplier
lineitem = db.lineitem
orders = db.orders
nation = db.nation

pipeline = [
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "S_SUPPKEY",
            "foreignField": "L_SUPPKEY",
            "as": "L1"
        }
    },
    { "$unwind": "$L1" },
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "L1.L_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "L2"
        }
    },
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "L1.L_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "L3"
        }
    },
    {
        "$lookup": {
            "from": "orders",
            "localField": "L1.L_ORDERKEY",
            "foreignField": "O_ORDERKEY",
            "as": "O1"
        }
    },
    {
        "$lookup": {
            "from": "nation",
            "localField": "S_NATIONKEY",
            "foreignField": "N_NATIONKEY",
            "as": "N1"
        }
    },
    {
        "$match": {
            "O1.O_ORDERSTATUS": "F",
            "L1.L_RECEIPTDATE": {"$gt": "L1.L_COMMITDATE"},
            "L2.L_SUPPKEY": {"$nin": ["$S_SUPPKEY"]},
            "L3": {"$eq": []},
            "N1.N_NAME": "SAUDI ARABIA"
        }
    },
    {
        "$group": {
            "_id": "$S_NAME",
            "NUMWAIT": { "$sum": 1 }
        }
    },
    {
        "$sort": {
            "NUMWAIT": -1,
            "_id": 1
        }
    }
]

results = supplier.aggregate(pipeline)

data = list(results)
df = pd.DataFrame(data)
df.to_csv('query_output.csv', index=False)
