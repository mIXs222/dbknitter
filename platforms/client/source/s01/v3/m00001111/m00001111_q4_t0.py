from pymongo import MongoClient
import csv

# Create a MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the pipelines for querying tables
orders_pipeline = [
    {"$match": {
        "O_ORDERDATE": {"$gte": "1993-07-01", "$lt": "1993-10-01"}
    }},
    {"$lookup": {
        "from": "lineitem",
        "localField": "O_ORDERKEY",
        "foreignField": "L_ORDERKEY",
        "as": "lineitem"
    }},
    {"$unwind": "$lineitem"},
    {"$match": {
        "lineitem.L_COMMITDATE": {"$lt": "lineitem.L_RECEIPTDATE"}
    }},
    {"$group": {
        "_id": "$O_ORDERPRIORITY",
        "ORDER_COUNT": {"$sum": 1}
    }},
    {"$sort": {
        "_id": 1
    }}
]

# Execute the query and fetch the result
orders = db['orders'].aggregate(orders_pipeline)

# Write the results to a csv file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERPRIORITY', 'ORDER_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for order in orders:
        writer.writerow({'O_ORDERPRIORITY': order['_id'], 'ORDER_COUNT': order['ORDER_COUNT']})
