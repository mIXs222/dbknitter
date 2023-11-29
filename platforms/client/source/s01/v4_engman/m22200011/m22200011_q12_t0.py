from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
orders = db['orders']
lineitem = db['lineitem']

# Initialize an empty list to store the results
results = []

# Define date range
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)

# Perform the query with aggregation
pipeline = [
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
        '$match': {
            'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
            'L_RECEIPTDATE': {'$gte': start_date, '$lt': end_date},
            'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
            'L_RECEIPTDATE': {'$gt': '$L_COMMITDATE'},
            'order_info.O_ORDERDATE': {'$gte': start_date, '$lt': end_date}
        }
    },
    {
        '$project': {
            'L_SHIPMODE': 1,
            'high_priority_count': {
                '$cond': {
                    'if': {'$in': ['$order_info.O_ORDERPRIORITY', ['URGENT', 'HIGH']]},
                    'then': 1,
                    'else': 0
                }
            },
            'low_priority_count': {
                '$cond': {
                    'if': {'$in': ['$order_info.O_ORDERPRIORITY', ['URGENT', 'HIGH']]},
                    'then': 0,
                    'else': 1
                }
            }
        }
    },
    {
        '$group': {
            '_id': '$L_SHIPMODE',
            'high_priority_total': {'$sum': '$high_priority_count'},
            'low_priority_total': {'$sum': '$low_priority_count'}
        }
    },
    {'$sort': {'_id': 1}}  # Sort by 'L_SHIPMODE' in ascending order
]

cursor = lineitem.aggregate(pipeline)

# Collecting results
for doc in cursor:
    results.append({
        'ship_mode': doc['_id'],
        'high_priority': doc['high_priority_total'],
        'low_priority': doc['low_priority_total']
    })

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['ship_mode', 'high_priority', 'low_priority'])
    writer.writeheader()
    for data in results:
        writer.writerow(data)

# Close MongoDB connection
client.close()
