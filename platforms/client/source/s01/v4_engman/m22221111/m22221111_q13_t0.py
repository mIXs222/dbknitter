# query.py

from pymongo import MongoClient
import csv

# MongoDB connection information
db_name = 'tpch'
port = 27017
hostname = 'mongodb'
client = MongoClient(hostname, port)
db = client[db_name]

# Query and aggregate data
pipeline = [
    {"$match": {
        "O_ORDERSTATUS": {"$nin": ["pending", "deposit"]},
        "O_COMMENT": {"$not": {"$regex": ".*pending.*deposits.*"}}
    }},
    {"$group": {
        "_id": "$O_CUSTKEY",
        "num_of_orders": {"$sum": 1}
    }},
    {"$group": {
        "_id": "$num_of_orders",
        "num_of_customers": {"$sum": 1}
    }},
    {"$project": {
        "number_of_orders": "$_id",
        "number_of_customers": "$num_of_customers",
        "_id": 0
    }},
    {"$sort": {"number_of_orders": 1}}
]

# Run aggregation
results = list(db.orders.aggregate(pipeline))

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['number_of_orders', 'number_of_customers']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow(result)

client.close()
