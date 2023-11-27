# query.py

from pymongo import MongoClient
import csv

# Connect to the MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Query to find orders with late lineitem
pipeline = [
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
            "O_ORDERDATE": {
                "$gte": "1993-07-01",
                "$lt": "1993-10-01"
            },
            "lineitems": {
                "$elemMatch": {
                    "L_RECEIPTDATE": {
                        "$gt": "$$lineitems.L_COMMITDATE"
                    }
                }
            }
        }
    },
    {
        "$group": {
            "_id": "$O_ORDERPRIORITY",
            "order_count": {
                "$sum": 1
            }
        }
    },
    {
        "$sort": {
            "_id": 1
        }
    }
]

# Execute the query
results = list(db.orders.aggregate(pipeline))

# Write results to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_ORDERPRIORITY', 'order_count'])
    for result in results:
        writer.writerow([result['_id'], result['order_count']])

print("Query results have been written to 'query_output.csv'")
