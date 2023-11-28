# query.py
import pymongo
import csv
from datetime import datetime

# Connect to MongoDB instance
client = pymongo.MongoClient(host="mongodb", port=27017)
db = client["tpch"]

# Allow MongoDB to understand we want to reference fields across documents
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'C_CUSTKEY',
            'foreignField': 'O_CUSTKEY',
            'as': 'orders'
        }
    },
    {'$unwind': '$orders'},
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'orders.O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    {'$unwind': '$lineitems'},
    {
        '$match': {
            'C_MKTSEGMENT': 'BUILDING',
            'orders.O_ORDERDATE': {'$lt': datetime(1995, 3, 15)},
            'lineitems.L_SHIPDATE': {'$gt': datetime(1995, 3, 15)}
        }
    },
    {
        '$project': {
            '_id': 0,
            'O_ORDERKEY': '$orders.O_ORDERKEY',
            'O_ORDERDATE': '$orders.O_ORDERDATE',
            'O_SHIPPRIORITY': '$orders.O_SHIPPRIORITY',
            'revenue': {
                '$subtract': [
                    '$lineitems.L_EXTENDEDPRICE',
                    {'$multiply': ['$lineitems.L_EXTENDEDPRICE', '$lineitems.L_DISCOUNT']}
                ]
            }
        }
    },
    {
        '$group': {
            '_id': {
                'O_ORDERKEY': '$O_ORDERKEY',
                'O_ORDERDATE': '$O_ORDERDATE',
                'O_SHIPPRIORITY': '$O_SHIPPRIORITY'
            },
            'total_revenue': {'$sum': '$revenue'}
        }
    },
    {
        '$sort': {
            'total_revenue': -1, 
            '_id.O_ORDERDATE': 1
        }
    }
]

# Execute the aggregation pipeline
results = db['customer'].aggregate(pipeline)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow({
            'O_ORDERKEY': result['_id']['O_ORDERKEY'],
            'O_ORDERDATE': result['_id']['O_ORDERDATE'].strftime('%Y-%m-%d'),
            'O_SHIPPRIORITY': result['_id']['O_SHIPPRIORITY'],
            'REVENUE': result['total_revenue']
        })
