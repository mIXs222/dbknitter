from pymongo import MongoClient
import csv
from datetime import datetime

# Database connection parameters
mongo_host = 'mongodb'
mongo_port = 27017
mongo_db_name = 'tpch'

# Connect to MongoDB
client = MongoClient(mongo_host, mongo_port)
db = client[mongo_db_name]

# Define the date range
start_date = datetime.strptime("1994-01-01", "%Y-%m-%d")
end_date = datetime.strptime("1995-01-01", "%Y-%m-%d")

# Aggregation pipeline for MongoDB query
pipeline = [
    {
        '$match': {
            'L_RECEIPTDATE': {'$gte': start_date, '$lt': end_date},
            'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
            'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']}
        }
    },
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'order'
        }
    },
    {'$unwind': '$order'},
    {
        '$project': {
            'L_SHIPMODE': 1,
            'L_RECEIPTDATE': 1,
            'L_COMMITDATE': 1,
            'O_ORDERPRIORITY': '$order.O_ORDERPRIORITY'
        }
    },
    {
        '$match': {
            'L_RECEIPTDATE': {'$gt': '$L_COMMITDATE'},
            'O_ORDERPRIORITY': {'$in': ['URGENT', 'HIGH']}
        }
    },
    {
        '$group': {
            '_id': {
                'L_SHIPMODE': '$L_SHIPMODE',
                'O_ORDERPRIORITY': '$O_ORDERPRIORITY'
            },
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id.L_SHIPMODE': 1, '_id.O_ORDERPRIORITY': -1}
    }
]

# Execute the query
results = list(db.lineitem.aggregate(pipeline))

# Write results to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['SHIPMODE', 'ORDERPRIORITY', 'COUNT'])
    for result in results:
        writer.writerow([result['_id']['L_SHIPMODE'], result['_id']['O_ORDERPRIORITY'], result['count']])
