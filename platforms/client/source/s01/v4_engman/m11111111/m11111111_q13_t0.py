from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient("mongodb", 27017)
db = client.tpch

# Query MongoDB to get customer order counts excluding pending and deposits
pipeline = [
    {"$match": {"O_COMMENT": {"$not": {"$regex": ".*pending.*deposits.*"}}}},
    {"$group": {"_id": "$O_CUSTKEY", "order_count": {"$sum": 1}}},
    {"$group": {"_id": "$order_count", "customer_count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
]

orders_count = db.orders.aggregate(pipeline)

# Write results to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Number of Orders', 'Number of Customers'])
    for doc in orders_count:
        writer.writerow([doc["_id"], doc["customer_count"]])

client.close()
