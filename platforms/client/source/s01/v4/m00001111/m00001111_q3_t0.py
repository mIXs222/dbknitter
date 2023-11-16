# query.py
from pymongo import MongoClient
import csv

# Establish MongoDB connection
client = MongoClient('mongodb://mongodb:27017/')
db = client['tpch']

# Aggregation pipeline to simulate the SQL query
pipeline = [
    # Join 'customer' with 'orders'
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'C_CUSTKEY',
            'foreignField': 'O_CUSTKEY',
            'as': 'orders'
        }
    },
    # Unwind the 'orders' output array
    {'$unwind': '$orders'},
    # Match 'orders' and 'customer' conditions
    {
        '$match': {
            'C_MKTSEGMENT': 'BUILDING',
            'orders.O_ORDERDATE': {'$lt': '1995-03-15'}
        }
    },
    # Join with 'lineitem'
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'orders.O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'lineitems'
        }
    },
    # Unwind the 'lineitems' output array
    {'$unwind': '$lineitems'},
    # Match 'lineitems' conditions
    {
        '$match': {
            'lineitems.L_SHIPDATE': {'$gt': '1995-03-15'}
        }
    },
    # Perform the calculation, group, and sort operations as instructed by the SQL query
    {
        '$group': {
            '_id': {
                'L_ORDERKEY': '$lineitems.L_ORDERKEY',
                'O_ORDERDATE': '$orders.O_ORDERDATE',
                'O_SHIPPRIORITY': '$orders.O_SHIPPRIORITY'
            },
            'REVENUE': {
                '$sum': {
                    '$multiply': [
                        '$lineitems.L_EXTENDEDPRICE',
                        {'$subtract': [1, '$lineitems.L_DISCOUNT']}
                    ]
                }
            },
        }
    },
    {
        '$sort': {
            'REVENUE': -1,
            '_id.O_ORDERDATE': 1
        }
    },
    # Include only the required fields
    {
        '$project': {
            'L_ORDERKEY': '$_id.L_ORDERKEY',
            'REVENUE': 1,
            'O_ORDERDATE': '$_id.O_ORDERDATE',
            'O_SHIPPRIORITY': '$_id.O_SHIPPRIORITY',
            '_id': 0
        }
    }
]

# Execute the aggregation pipeline and fetch the result
results = list(db['customer'].aggregate(pipeline))

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in results:
        writer.writerow(data)
