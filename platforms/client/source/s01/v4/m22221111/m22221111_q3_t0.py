import pymongo
import csv
from decimal import Decimal

# Establish a connection to the MongoDB database
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]

# Retrieve relevant collections
customer_collection = mongodb["customer"]
orders_collection = mongodb["orders"]
lineitem_collection = mongodb["lineitem"]

# Perform the query using aggregations in MongoDB
pipeline = [
    {
        "$match": {
            "C_MKTSEGMENT": "BUILDING",
        }
    },
    {
        "$lookup": {
            "from": "orders",
            "localField": "C_CUSTKEY",
            "foreignField": "O_CUSTKEY",
            "as": "orders"
        }
    },
    {"$unwind": "$orders"},
    {
        "$match": {
            "orders.O_ORDERDATE": {"$lt": "1995-03-15"}
        }
    },
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "orders.O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "lineitems"
        }
    },
    {"$unwind": "$lineitems"},
    {
        "$match": {
            "lineitems.L_SHIPDATE": {"$gt": "1995-03-15"}
        }
    },
    {
        "$project": {
            "L_ORDERKEY": "$lineitems.L_ORDERKEY",
            "REVENUE": {
                "$multiply": [
                    "$lineitems.L_EXTENDEDPRICE",
                    {"$subtract": [1, "$lineitems.L_DISCOUNT"]}
                ]
            },
            "O_ORDERDATE": "$orders.O_ORDERDATE",
            "O_SHIPPRIORITY": "$orders.O_SHIPPRIORITY"
        }
    },
    {
        "$group": {
            "_id": {
                "L_ORDERKEY": "$L_ORDERKEY",
                "O_ORDERDATE": "$O_ORDERDATE",
                "O_SHIPPRIORITY": "$O_SHIPPRIORITY"
            },
            "REVENUE": {"$sum": "$REVENUE"}
        }
    },
    {
        "$sort": {
            "REVENUE": -1,
            "O_ORDERDATE": 1
        }
    }
]

result = list(customer_collection.aggregate(pipeline))

# Write to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])

    for data in result:
        orderkey = data["_id"]["L_ORDERKEY"]
        revenue = round(Decimal(data["REVENUE"]), 2)
        orderdate = data["_id"]["O_ORDERDATE"]
        shippriority = data["_id"]["O_SHIPPRIORITY"]
        writer.writerow([orderkey, revenue, orderdate, shippriority])

print("Query output has been written to query_output.csv")
