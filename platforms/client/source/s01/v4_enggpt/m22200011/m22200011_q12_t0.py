# query.py

import pymongo
import csv
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']

# MongoDB aggregation pipeline
pipeline = [
    {
        '$match': {
            'O_ORDERDATE': {
                '$gte': datetime(1994, 1, 1),
                '$lte': datetime(1994, 12, 31)
            }
        }
    },
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {
        '$unwind': '$lineitems'
    },
    {
        '$match': {
            'lineitems.L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
            'lineitems.L_COMMITDATE': {'$lt': '$lineitems.L_RECEIPTDATE'},
            'lineitems.L_SHIPDATE': {'$lt': '$lineitems.L_COMMITDATE'}
        }
    },
    {
        '$project': {
            'priority': {
                '$cond': {
                    'if': {'$in': ['$O_ORDERPRIORITY', ['1-URGENT', '2-HIGH']]},
                    'then': 'HIGH_LINE_COUNT',
                    'else': 'LOW_LINE_COUNT'
                }
            },
            'L_SHIPMODE': '$lineitems.L_SHIPMODE'
        }
    },
    {
        '$group': {
            '_id': {
                'L_SHIPMODE': '$L_SHIPMODE',
                'priority': '$priority'
            },
            'count': {'$sum': 1}
        }
    },
    {
        '$group': {
            '_id': '$_id.L_SHIPMODE',
            'counts': {
                '$push': {
                    'priority': '$_id.priority',
                    'count': '$count'
                }
            }
        }
    },
    {
        '$sort': {'_id': 1}
    }
]

# Execute aggregation pipeline
results = db.orders.aggregate(pipeline)

# Write to CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    
    for result in results:
        high_count = next((item['count'] for item in result['counts'] if item['priority'] == 'HIGH_LINE_COUNT'), 0)
        low_count = next((item['count'] for item in result['counts'] if item['priority'] == 'LOW_LINE_COUNT'), 0)
        writer.writerow([result['_id'], high_count, low_count])
        
print("Output written to query_output.csv")
