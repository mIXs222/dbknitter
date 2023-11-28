# mongo_query.py
from pymongo import MongoClient
import csv
import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Perform the analysis
orders_coll = db['orders']
lineitem_coll = db['lineitem']

# Define the date range for filter
start_date = datetime.datetime(1994, 1, 1)
end_date = datetime.datetime(1994, 12, 31)

# Create a pipeline for aggregation
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
            'O_ORDERPRIORITY': {'$in': ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-LOW', '5-NOT SPECIFIED']},
            'lineitems.L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
            'lineitems.L_COMMITDATE': {'$lt': '$lineitems.L_RECEIPTDATE'},
            'lineitems.L_SHIPDATE': {'$lt': '$lineitems.L_COMMITDATE'},
            'lineitems.L_RECEIPTDATE': {'$gte': start_date, '$lte': end_date}
        },
    },
    {
        '$group': {
            '_id': {
                'ship_mode': '$lineitems.L_SHIPMODE',
                'priority': {
                    '$cond': [{'$in': ['$O_ORDERPRIORITY', ['1-URGENT', '2-HIGH']]}, 'HIGH', 'LOW']
                },
            },
            'line_count': {'$sum': 1}
        }
    },
    {
        '$group': {
            '_id': '$_id.ship_mode',
            'high_line_count': {
                '$sum': {
                    '$cond': [{'$eq': ['$_id.priority', 'HIGH']}, '$line_count', 0]
                }
            },
            'low_line_count': {
                '$sum': {
                    '$cond': [{'$eq': ['$_id.priority', 'LOW']}, '$line_count', 0]
                }
            }
        }
    },
    {'$sort': {'_id': 1}} # Sort by shipping mode
]

results = list(orders_coll.aggregate(pipeline))

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['ship_mode', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])

    for result in results:
        writer.writerow([result['_id'], result['high_line_count'], result['low_line_count']])

# Close the MongoClient
client.close()
