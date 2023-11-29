from pymongo import MongoClient
import csv

# MongoDB connection parameters
DB_NAME = 'tpch'
PORT = 27017
HOSTNAME = 'mongodb'

# Establish connection
client = MongoClient(HOSTNAME, PORT)
db = client[DB_NAME]

# Define the aggregation pipeline
pipeline = [
    {
        '$lookup': {
            'from': 'orders',
            'localField': 'O_CUSTKEY',
            'foreignField': 'C_CUSTKEY',
            'as': 'order'
        }
    },
    {'$unwind': '$order'},
    {
        '$match': {
            'C_MKTSEGMENT': 'BUILDING',
            'order.O_ORDERDATE': {'$lt': '1995-03-05'},
            '$expr': {'$gt': ['$L_SHIPDATE', '1995-03-15']}
        }
    },
    {
        '$project': {
            'O_ORDERKEY': '$order.O_ORDERKEY',
            'REVENUE': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]},
            'O_ORDERDATE': '$order.O_ORDERDATE',
            'O_SHIPPRIORITY': '$order.O_SHIPPRIORITY'
        }
    },
    {'$sort': {'REVENUE': -1}},
    {'$group': {
        '_id': '$O_ORDERKEY',
        'REVENUE': {'$sum': '$REVENUE'},
        'O_ORDERDATE': {'$first': '$O_ORDERDATE'},
        'O_SHIPPRIORITY': {'$first': '$O_SHIPPRIORITY'}
    }},
    {'$project': {
        '_id': 0,
        'O_ORDERKEY': '$_id',
        'REVENUE': 1,
        'O_ORDERDATE': 1,
        'O_SHIPPRIORITY': 1
    }}
]

# Execute the aggregation pipeline
results = db['lineitem'].aggregate(pipeline)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow(result)
