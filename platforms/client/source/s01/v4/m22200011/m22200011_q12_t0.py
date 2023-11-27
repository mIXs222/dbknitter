# query.py

from pymongo import MongoClient
import csv
import datetime

# Connect to the MongoDB instance
client = MongoClient('mongodb', 27017)
mongo_db = client['tpch']

# Retrieve 'orders' and 'lineitem' collections
orders = mongo_db.orders
lineitems = mongo_db.lineitem

# Perform the aggregation
pipeline = [
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {'$unwind': '$lineitems'},
    {
        '$match': {
            'lineitems.L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
            'lineitems.L_COMMITDATE': {'$lt': '$lineitems.L_RECEIPTDATE'},
            'lineitems.L_SHIPDATE': {'$lt': '$lineitems.L_COMMITDATE'},
            'lineitems.L_RECEIPTDATE': {
                '$gte': datetime.datetime(1994, 1, 1),
                '$lt': datetime.datetime(1995, 1, 1)
            }
        }
    },
    {
        '$group': {
            '_id': '$lineitems.L_SHIPMODE',
            'HIGH_LINE_COUNT': {
                '$sum': {
                    '$cond': [
                        {'$or': [
                            {'$eq': ['$O_ORDERPRIORITY', '1-URGENT']},
                            {'$eq': ['$O_ORDERPRIORITY', '2-HIGH']}
                        ]},
                        1,
                        0
                    ]
                }
            },
            'LOW_LINE_COUNT': {
                '$sum': {
                    '$cond': [
                        {
                            '$and': [
                                {'$ne': ['$O_ORDERPRIORITY', '1-URGENT']},
                                {'$ne': ['$O_ORDERPRIORITY', '2-HIGH']}
                            ]
                        },
                        1,
                        0
                    ]
                }
            }
        }
    },
    {'$sort': {'_id': 1}}
]

results = orders.aggregate(pipeline)

# Write the results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    
    for result in results:
        writer.writerow([result['_id'], result['HIGH_LINE_COUNT'], result['LOW_LINE_COUNT']])
