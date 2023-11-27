from pymongo import MongoClient
import csv

# MongoDB connection string
client = MongoClient('mongodb', 27017)
db = client["tpch"]

# Aggregation pipeline for MongoDB query
pipeline = [
    {
        "$match": {
            "O_ORDERDATE": {"$lt": "1995-03-15"}, 
            "O_ORDERSTATUS": {"$ne": "shipped"}
        }
    },
    {
        "$lookup": {
            "from": "customer",
            "localField": "O_CUSTKEY",
            "foreignField": "C_CUSTKEY",
            "as": "customer_info"
        }
    },
    {
        "$unwind": "$customer_info"
    },
    {
        "$match": {
            "customer_info.C_MKTSEGMENT": "BUILDING"
        }
    },
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "line_items"
        }
    },
    {
        "$unwind": "$line_items"
    },
    {
        "$group": {
            "_id": "$O_ORDERKEY",
            "O_SHIPPRIORITY": {"$first": "$O_SHIPPRIORITY"},
            "revenue": {
                "$sum": {
                    "$multiply": [
                        "$line_items.L_EXTENDEDPRICE",
                        {"$subtract": [1, "$line_items.L_DISCOUNT"]}
                    ]
                }
            }
        }
    },
    {
        "$sort": {
            "revenue": -1
        }
    }
]

# Execute query
result = db["orders"].aggregate(pipeline)

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERKEY', 'O_SHIPPRIORITY', 'revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in result:
        writer.writerow({'O_ORDERKEY': data['_id'], 'O_SHIPPRIORITY': data['O_SHIPPRIORITY'], 'revenue': data['revenue']})
