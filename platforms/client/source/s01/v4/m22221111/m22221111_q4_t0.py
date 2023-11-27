# mongodb_query.py
from pymongo import MongoClient
import csv

# MongoDB connection setup
client = MongoClient('mongodb', 27017)
db = client['tpch']
orders_collection = db['orders']
lineitem_collection = db['lineitem']

# Perform the query
pipeline = [
    {
        "$match": {
            "O_ORDERDATE": {
                "$gte": "1993-07-01",
                "$lt": "1993-10-01"
            }
        }
    },
    {
        "$lookup": {
            "from": "lineitem",
            "let": {"orderkey": "$O_ORDERKEY"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$L_ORDERKEY", "$$orderkey"]},
                                {"$lt": ["$L_COMMITDATE", "$L_RECEIPTDATE"]}
                            ]
                        }
                    }
                }
            ],
            "as": "lineitems"
        }
    },
    {
        "$match": {
            "lineitems": {"$exists": True, "$ne": []}
        }
    },
    {
        "$group": {
            "_id": "$O_ORDERPRIORITY",
            "ORDER_COUNT": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": 1}
    }
]

results = orders_collection.aggregate(pipeline)

# Writing the result to a CSV file
with open('query_output.csv', mode='w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["O_ORDERPRIORITY", "ORDER_COUNT"])  # header row
    
    for document in results:
        csvwriter.writerow([document["_id"], document["ORDER_COUNT"]])
