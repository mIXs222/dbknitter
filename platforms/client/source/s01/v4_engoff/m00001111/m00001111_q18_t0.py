from pymongo import MongoClient
import csv

# MongoDB connection
client = MongoClient('mongodb', 27017)
db = client.tpch

# Aggregation pipeline to execute the query
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
        '$group': {
            '_id': {
                'C_CUSTKEY': '$C_CUSTKEY', 
                'C_NAME': '$C_NAME', 
                'O_ORDERKEY': '$orders.O_ORDERKEY', 
                'O_ORDERDATE': '$orders.O_ORDERDATE', 
                'O_TOTALPRICE': '$orders.O_TOTALPRICE'
            },
            'total_qty': {'$sum': '$lineitems.L_QUANTITY'}
        }
    },
    {'$match': {'total_qty': {'$gt': 300}}},
    {
        '$project': {
            '_id': 0, 
            'C_NAME': '$_id.C_NAME', 
            'C_CUSTKEY': '$_id.C_CUSTKEY',
            'O_ORDERKEY': '$_id.O_ORDERKEY', 
            'O_ORDERDATE': '$_id.O_ORDERDATE', 
            'O_TOTALPRICE': '$_id.O_TOTALPRICE',
            'total_qty': 1
        }
    }
]

# Write the query output to a file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_qty']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    # Run the pipeline
    for result in db.customer.aggregate(pipeline):
        writer.writerow(result)
