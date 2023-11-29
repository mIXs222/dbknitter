# mongo_query.py
from pymongo import MongoClient
import csv
import datetime

# Connect to MongoDB instance
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Aggregation pipeline for MongoDB
pipeline = [
    {
        '$lookup': {
            'from': 'customer',
            'localField': 'O_CUSTKEY',
            'foreignField': 'C_CUSTKEY',
            'as': 'customer_info'
        }
    },
    {'$unwind': '$customer_info'},
    {
        '$match': {
            'O_ORDERDATE': {'$lt': datetime.datetime(1995, 3, 5)},
            'customer_info.C_MKTSEGMENT': 'BUILDING'
        }
    },
    {
        '$lookup': {
            'from': 'lineitem',
            'localField': 'O_ORDERKEY',
            'foreignField': 'L_ORDERKEY',
            'as': 'line_items'
        }
    },
    {'$unwind': '$line_items'},
    {
        '$match': {
            'line_items.L_SHIPDATE': {'$gt': datetime.datetime(1995, 3, 15)}
        }
    },
    {
        '$project': {
            '_id': 0,
            'O_ORDERKEY': 1,
            'O_ORDERDATE': 1,
            'O_SHIPPRIORITY': 1,
            'REVENUE': {
                '$multiply': [
                    '$line_items.L_EXTENDEDPRICE', 
                    {'$subtract': [1, '$line_items.L_DISCOUNT']}
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
            'REVENUE': {'$sum': '$REVENUE'}
        }
    },
    {
        '$project': {
            '_id': 0,
            'O_ORDERKEY': '$_id.O_ORDERKEY',
            'O_ORDERDATE': '$_id.O_ORDERDATE',
            'O_SHIPPRIORITY': '$_id.O_SHIPPRIORITY',
            'REVENUE': '$REVENUE'
        }
    },
    {'$sort': {'REVENUE': -1}}
]

# Execute the query
results = db.orders.aggregate(pipeline)

# Write the output to a CSV file
with open("query_output.csv", "w", newline="") as csvfile:
    fieldnames = ['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        # Formatting dates to match SQL output
        result['O_ORDERDATE'] = result['O_ORDERDATE'].strftime('%Y-%m-%d')
        writer.writerow(result)

# Close the MongoDB client
client.close()
