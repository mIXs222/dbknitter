import csv
from pymongo import MongoClient
from datetime import datetime

# Function to convert a string to a python datetime
def str_to_date(date_str):
    return datetime.strptime(date_str, '%Y-%m-%d')

# MongoDB connection details.
mongodb_host = 'mongodb'
mongodb_port = 27017
mongodb_db_name = 'tpch'

# Connect to the MongoDB server.
client = MongoClient(mongodb_host, mongodb_port)
db = client[mongodb_db_name]

# Querying the collections and filtering the data.
orders_collection = db['orders']
lineitem_collection = db['lineitem']

# Define the needed dates as datetime objects for comparison.
start_date = str_to_date('1994-01-01')
end_date = str_to_date('1995-01-01')

# Aggregation pipeline for MongoDB.
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'order_info',
        }
    },
    {
        '$match': {
            'L_RECEIPTDATE': {'$gte': start_date, '$lte': end_date},
            'L_COMMITDATE': {'$lt': '$L_RECEIPTDATE'},
            'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
            'L_SHIPMODE': {'$in': ['mail', 'ship']},
            'order_info.O_ORDERPRIORITY': {'$in': ['1-URGENT', '2-HIGH']}
        }
    },
    {
        '$project': {
            'L_SHIPMODE': 1,
            'order_priority': {
                '$cond': {
                    'if': {'$in': ['$order_info.O_ORDERPRIORITY', ['1-URGENT', '2-HIGH']]},
                    'then': 'HIGH',
                    'else': 'LOW'
                }
            },
        }
    },
    {
        '$group': {
            '_id': '$L_SHIPMODE',
            'high_order_priority_count': {
                '$sum': {
                    '$cond': [{'$eq': ['$order_priority', 'HIGH']}, 1, 0]
                }
            },
            'low_order_priority_count': {
                '$sum': {
                    '$cond': [{'$eq': ['$order_priority', 'LOW']}, 1, 0]
                }
            }
        }
    },
    {
        '$sort': {
            '_id': 1
        }
    }
]

# Execute the aggregation pipeline
results = lineitem_collection.aggregate(pipeline)

# Write the results to a CSV file.
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_SHIPMODE', 'high_order_priority_count', 'low_order_priority_count']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow({
            'L_SHIPMODE': result['_id'],
            'high_order_priority_count': result['high_order_priority_count'],
            'low_order_priority_count': result['low_order_priority_count']
        })
