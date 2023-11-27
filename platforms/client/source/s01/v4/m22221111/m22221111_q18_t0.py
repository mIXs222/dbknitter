from pymongo import MongoClient
import csv

# Function to connect to MongoDB
def connect_to_mongodb():
    client = MongoClient('mongodb', 27017)
    db = client['tpch']
    return db

# Connect to MongoDB
db = connect_to_mongodb()

# Aggregation pipeline for MongoDB
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
                'C_NAME': '$C_NAME', 'C_CUSTKEY': '$C_CUSTKEY',
                'O_ORDERKEY': '$orders.O_ORDERKEY', 'O_ORDERDATE': '$orders.O_ORDERDATE',
                'O_TOTALPRICE': '$orders.O_TOTALPRICE'
            },
            'TOTAL_QUANTITY': {'$sum': '$lineitems.L_QUANTITY'}
        }
    },
    {'$match': {'TOTAL_QUANTITY': {'$gt': 300}}},
    {
        '$project': {
            '_id': 0,
            'C_NAME': '$_id.C_NAME',
            'C_CUSTKEY': '$_id.C_CUSTKEY',
            'O_ORDERKEY': '$_id.O_ORDERKEY',
            'O_ORDERDATE': '$_id.O_ORDERDATE',
            'O_TOTALPRICE': '$_id.O_TOTALPRICE',
            'TOTAL_QUANTITY': 1
        }
    },
    {'$sort': {'O_TOTALPRICE': -1, 'O_ORDERDATE': 1}}
]

# Execute the query
results = db['customer'].aggregate(pipeline)

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM(L_QUANTITY)']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow({
            'C_NAME': result['C_NAME'],
            'C_CUSTKEY': result['C_CUSTKEY'],
            'O_ORDERKEY': result['O_ORDERKEY'],
            'O_ORDERDATE': result['O_ORDERDATE'],
            'O_TOTALPRICE': result['O_TOTALPRICE'],
            'SUM(L_QUANTITY)': result['TOTAL_QUANTITY']
        })
