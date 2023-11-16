import pymongo
import csv
from datetime import datetime

# Connect to the mongodb server
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client.tpch

# Querying the mongodb database
pipeline = [
    {
        '$match': {
            'l_shipmode': {'$in': ['MAIL', 'SHIP']},
            'l_commitdate': {'$lt': '$l_receiptdate'},
            'l_shipdate': {'$lt': '$l_commitdate'},
            'l_receiptdate': {'$gte': datetime(1994, 1, 1), '$lt': datetime(1995, 1, 1)}
        }
    },
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'l_orderkey',
            'foreignField': 'o_orderkey',
            'as': 'order_info'
        }
    },
    {'$unwind': '$order_info'},
    {
        '$group': {
            '_id': '$l_shipmode',
            'HIGH_LINE_COUNT': {
                '$sum': {
                    '$cond': [
                        {'$or': [
                            {'$eq': ['$order_info.o_orderpriority', '1-URGENT']},
                            {'$eq': ['$order_info.o_orderpriority', '2-HIGH']}
                        ]}, 1, 0
                    ]
                }
            },
            'LOW_LINE_COUNT': {
                '$sum': {
                    '$cond': [
                        {'$and': [
                            {'$ne': ['$order_info.o_orderpriority', '1-URGENT']},
                            {'$ne': ['$order_info.o_orderpriority', '2-HIGH']}
                        ]}, 1, 0
                    ]
                }
            }
        }
    },
    {
        '$sort': {'_id': 1}
    }
]

results = db.lineitem.aggregate(pipeline)

# Write query's output to a CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])

    for result in results:
        writer.writerow([result['_id'], result['HIGH_LINE_COUNT'], result['LOW_LINE_COUNT']])
