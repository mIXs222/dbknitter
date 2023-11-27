# query.py
from pymongo import MongoClient
import csv
from datetime import datetime

# Establish a connection to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client.tpch

# Perform the query
output_data = db.orders.aggregate([
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "lineitems"
        }
    },
    {
        "$match": {
            "O_ORDERDATE": {"$lt": datetime(1995, 3, 15)},
            "O_ORDERSTATUS": {"$ne": "Shipped"},
        }
    },
    {"$unwind": "$lineitems"},
    {
        "$lookup": {
            "from": "customer",
            "localField": "O_CUSTKEY",
            "foreignField": "C_CUSTKEY",
            "as": "customer_info"
        }
    },
    {"$unwind": "$customer_info"},
    {
        "$match": {
            "customer_info.C_MKTSEGMENT": "BUILDING"
        }
    },
    {
        "$project": {
            "O_ORDERKEY": 1,
            "O_SHIPPRIORITY": 1,
            "revenue": {
                "$multiply": [
                    "$lineitems.L_EXTENDEDPRICE", {"$subtract": [1, "$lineitems.L_DISCOUNT"]}
                ]
            }
        }
    },
    {
        "$group": {
            "_id": {"O_ORDERKEY": "$O_ORDERKEY", "O_SHIPPRIORITY": "$O_SHIPPRIORITY"},
            "total_revenue": {"$sum": "$revenue"}
        }
    },
    {
        "$sort": {"total_revenue": -1}
    },
    {
        "$limit": 1
    },
    {
        "$project": {
            "_id": 0,
            "O_ORDERKEY": "$_id.O_ORDERKEY",
            "O_SHIPPRIORITY": "$_id.O_SHIPPRIORITY",
            "total_revenue": 1
        }
    }
])

# Write query results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=["O_ORDERKEY", "O_SHIPPRIORITY", "total_revenue"])
    writer.writeheader()
    for data in output_data:
        writer.writerow(data)
