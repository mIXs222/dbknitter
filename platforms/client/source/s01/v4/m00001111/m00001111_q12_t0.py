import pymongo
import csv
from datetime import datetime

# Establish connection to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client['tpch']

# Access the 'orders' and 'lineitem' collections
orders_collection = db['orders']
lineitem_collection = db['lineitem']

# Prepare the pipeline for the aggregate query
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
        '$unwind': '$lineitems'
    },
    {
        '$match': {
            'lineitems.L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
            'lineitems.L_COMMITDATE': {'$lt': '$lineitems.L_RECEIPTDATE'},
            'lineitems.L_SHIPDATE': {'$lt': '$lineitems.L_COMMITDATE'},
            'lineitems.L_RECEIPTDATE': {
                '$gte': datetime(1994, 1, 1),
                '$lt': datetime(1995, 1, 1)
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
        '$sort': {'_id': 1}
    }
]

# Execute the aggregation pipeline
results = orders_collection.aggregate(pipeline)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow({
            'L_SHIPMODE': result['_id'],
            'HIGH_LINE_COUNT': result['HIGH_LINE_COUNT'],
            'LOW_LINE_COUNT': result['LOW_LINE_COUNT']
        })
