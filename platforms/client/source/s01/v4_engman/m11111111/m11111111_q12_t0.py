from pymongo import MongoClient
from datetime import datetime
import csv

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
orders_collection = db['orders']
lineitem_collection = db['lineitem']

# Query parameters
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
ship_modes = ['MAIL', 'SHIP']

# Perform query
pipeline = [
    {
        '$match': {
            'L_RECEIPTDATE': {'$gte': start_date, '$lt': end_date},
            'L_SHIPMODE': {'$in': ship_modes},
            'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
            'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'}
        }
    },
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'order_info'
        }
    },
    {'$unwind': '$order_info'},
    {
        '$project': {
            'L_SHIPMODE': 1,
            'order_priority_high': {
                '$cond': {'if': {'$in': ['$order_info.O_ORDERPRIORITY', ['URGENT', 'HIGH']]}, 'then': 1, 'else': 0}
            },
            'order_priority_low': {
                '$cond': {'if': {'$nin': ['$order_info.O_ORDERPRIORITY', ['URGENT', 'HIGH']]}, 'then': 1, 'else': 0}
            }
        }
    },
    {
        '$group': {
            '_id': '$L_SHIPMODE',
            'high_priority_count': {'$sum': '$order_priority_high'},
            'low_priority_count': {'$sum': '$order_priority_low'}
        }
    },
    {'$sort': {'_id': 1}}
]

results = list(lineitem_collection.aggregate(pipeline))

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['ship_mode', 'high_priority_count', 'low_priority_count']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow({
            'ship_mode': result['_id'],
            'high_priority_count': result['high_priority_count'],
            'low_priority_count': result['low_priority_count']
        })
