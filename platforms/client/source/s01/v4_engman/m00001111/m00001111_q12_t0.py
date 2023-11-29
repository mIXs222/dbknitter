from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Query against MongoDB
query_result = db.lineitem.aggregate([
    {
        "$match": {
            "L_RECEIPTDATE": {"$gte": datetime(1994, 1, 1), "$lt": datetime(1995, 1, 1)},
            "L_COMMITDATE": {"$lt": "$L_RECEIPTDATE"},
            "L_SHIPDATE": {"$lt": "$L_COMMITDATE"},
            "L_SHIPMODE": {"$in": ["MAIL", "SHIP"]}
        }
    },
    {
        "$lookup": {
            "from": "orders",
            "localField": "L_ORDERKEY",
            "foreignField": "O_ORDERKEY",
            "as": "order_docs"
        }
    },
    {
        "$match": {
            "order_docs.O_ORDERPRIORITY": {"$in": ["1-URGENT", "2-HIGH"]}
        }
    },
    {
        "$group": {
            "_id": "$L_SHIPMODE",
            "high_priority_count": {
                "$sum": {
                    "$cond": [
                        {"$or": [
                            {"$eq": ["$order_docs.O_ORDERPRIORITY", "1-URGENT"]},
                            {"$eq": ["$order_docs.O_ORDERPRIORITY", "2-HIGH"]}
                        ]},
                        1,
                        0
                    ]
                }
            },
            "low_priority_count": {
                "$sum": {
                    "$cond": [
                        {"$and": [
                            {"$ne": ["$order_docs.O_ORDERPRIORITY", "1-URGENT"]},
                            {"$ne": ["$order_docs.O_ORDERPRIORITY", "2-HIGH"]}
                        ]},
                        1,
                        0
                    ]
                }
            }
        }
    },
    {
        "$sort": {"_id": 1}
    }
])

# Write results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['ship_mode', 'high_priority_count', 'low_priority_count'])

    for doc in query_result:
        writer.writerow([doc['_id'], doc['high_priority_count'], doc['low_priority_count']])

# Close the MongoDB client
client.close()
