from pymongo import MongoClient
import csv

# MongoDB connection parameters
HOSTNAME = 'mongodb'
PORT = 27017
DATABASE_NAME = 'tpch'

# Connect to MongoDB
client = MongoClient(HOSTNAME, PORT)
db = client[DATABASE_NAME]

# Define the date range
date_start = "1993-10-01"
date_end = "1993-12-31"

# Aggregation pipeline
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
            'lineitems.L_RETURNFLAG': 'R',
            'orders.O_ORDERDATE': {'$gte': date_start, '$lt': date_end}
        }
    },
    {
        '$lookup': {
            'from': 'nation',
            'localField': 'C_NATIONKEY',
            'foreignField': 'N_NATIONKEY',
            'as': 'nation'
        }
    },
    {'$unwind': '$nation'},
    {
        '$group': {
            '_id': {
                'C_CUSTKEY': '$C_CUSTKEY',
                'C_NAME': '$C_NAME',
                'C_ACCTBAL': '$C_ACCTBAL',
                'C_PHONE': '$C_PHONE',
                'N_NAME': '$nation.N_NAME',
                'C_ADDRESS': '$C_ADDRESS',
                'C_COMMENT': '$C_COMMENT',
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
            'REVENUE': 1, '_id.C_CUSTKEY': 1, '_id.C_NAME': 1, '_id.C_ACCTBAL': -1
        }
    }
]

# Execute the aggregation query
results = list(db.customer.aggregate(pipeline))

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = [
        'C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL',
        'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow({
            'C_CUSTKEY': result['_id']['C_CUSTKEY'],
            'C_NAME': result['_id']['C_NAME'],
            'REVENUE': result['REVENUE'],
            'C_ACCTBAL': result['_id']['C_ACCTBAL'],
            'N_NAME': result['_id']['N_NAME'],
            'C_ADDRESS': result['_id']['C_ADDRESS'],
            'C_PHONE': result['_id']['C_PHONE'],
            'C_COMMENT': result['_id']['C_COMMENT']
        })

# Close the MongoDB client connection
client.close()
