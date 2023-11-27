from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the query period
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)

# Aggregation query for MongoDB
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'L_ORDERKEY',
            'foreignField': 'O_ORDERKEY',
            'as': 'order_docs'
        }
    },
    {
        '$match': {
            'L_SHIPDATE': {'$lt': '$L_COMMITDATE'},
            'L_RECEIPTDATE': {'$gte': start_date, '$lt': end_date},
            'L_RECEIPTDATE': {'$gt': '$L_COMMITDATE'},
            'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']}
        }
    },
    {
        '$project': {
            'L_SHIPMODE': 1,
            'O_ORDERPRIORITY': '$order_docs.O_ORDERPRIORITY'
        }
    },
    {
        '$unwind': '$O_ORDERPRIORITY'
    },
    {
        '$group': {
            '_id': {
                'L_SHIPMODE': '$L_SHIPMODE',
                'PriorityGroup': {
                    '$cond': {
                        'if': {'$in': ['$O_ORDERPRIORITY', ['URGENT', 'HIGH']]},
                        'then': 'URGENT/HIGH',
                        'else': 'OTHER'
                    }
                }
            },
            'LateLineItemCount': {'$sum': 1}
        }
    },
    {
        '$sort': {'_id.L_SHIPMODE': 1, '_id.PriorityGroup': -1}
    },
    {
        '$project': {
            'L_SHIPMODE': '$_id.L_SHIPMODE',
            'PriorityGroup': '$_id.PriorityGroup',
            'LateLineItemCount': '$LateLineItemCount',
            '_id': 0
        }
    }
]

# Run the aggregation query
results = list(db['lineitem'].aggregate(pipeline))

# Write results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['L_SHIPMODE', 'PriorityGroup', 'LateLineItemCount'])
    writer.writeheader()
    writer.writerows(results)
