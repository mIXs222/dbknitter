import csv
from pymongo import MongoClient
from datetime import datetime


# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Access the orders and lineitem collections
orders_collection = db['orders']
lineitem_collection = db['lineitem']

# Join operation in MongoDB can be quite intensive, it is advisable to use Aggregation Framework
pipeline = [
    {
        '$match': {
            'O_ORDERDATE': {
                '$gte': datetime(1994, 1, 1),
                '$lt': datetime(1995, 1, 1)
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
    {'$unwind': '$lineitems'},
    {
        '$match': {
            'lineitems.L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
            'lineitems.L_COMMITDATE': {'$lt': '$lineitems.L_RECEIPTDATE'},
            'lineitems.L_SHIPDATE': {'$lt': '$lineitems.L_COMMITDATE'},
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
                        {'$and': [
                            {'$ne': ['$O_ORDERPRIORITY', '1-URGENT']},
                            {'$ne': ['$O_ORDERPRIORITY', '2-HIGH']}
                        ]},
                        1,
                        0
                    ]
                }
            }
        }
    },
    {
        '$project': {
            'L_SHIPMODE': '$_id',
            'HIGH_LINE_COUNT': '$HIGH_LINE_COUNT',
            'LOW_LINE_COUNT': '$LOW_LINE_COUNT',
            '_id': 0
        }
    },
    {
        '$sort': {
            'L_SHIPMODE': 1
        }
    }
]

# Run the aggregation pipeline
result = list(orders_collection.aggregate(pipeline))

# Write the output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    writer.writeheader()
    for data in result:
        writer.writerow(data)
