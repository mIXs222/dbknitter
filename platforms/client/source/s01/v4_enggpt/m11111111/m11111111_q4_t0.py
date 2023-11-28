from pymongo import MongoClient
import csv
from datetime import datetime

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Convert string date to datetime object for comparison
start_date = datetime.strptime('1993-07-01', '%Y-%m-%d')
end_date = datetime.strptime('1993-10-01', '%Y-%m-%d')

# Aggregation pipeline for MongoDB
pipeline = [
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {
        '$match': {
            'O_ORDERDATE': {'$gte': start_date, '$lte': end_date},
            'lineitems': {
                '$elemMatch': {
                    'L_COMMITDATE': {'$lt': '$$ROOT.L_RECEIPTDATE'}
                }
            }
        }
    },
    {'$unwind': '$lineitems'},
    {
        '$match': {
            'lineitems.L_COMMITDATE': {'$lt': 'lineitems.L_RECEIPTDATE'}
        }
    },
    {
        '$group': {
            '_id': '$O_ORDERPRIORITY',
            'count': {'$sum': 1}
        }
    },
    {'$sort': {'_id': 1}}
]

# Run aggregation and get the results
results = db.orders.aggregate(pipeline)

# Write to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['order_priority', 'count'])  # Header

    for result in results:
        writer.writerow([result['_id'], result['count']])
