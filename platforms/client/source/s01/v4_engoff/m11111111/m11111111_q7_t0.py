import pymongo
import csv
from datetime import datetime

# Constants for the query
INDIA = "INDIA"
JAPAN = "JAPAN"
START_YEAR = 1995
END_YEAR = 1996

# Connection info for MongoDB
MONGO_DB_NAME = 'tpch'
MONGO_PORT = 27017
MONGO_HOSTNAME = 'mongodb'

# Connect to the mongodb server
client = pymongo.MongoClient(host=MONGO_HOSTNAME, port=MONGO_PORT)
db = client[MONGO_DB_NAME]

# Prepare the aggregation query
pipeline = [
    {
        '$lookup': {
            'from': 'supplier',
            'localField': 'L_SUPPKEY',
            'foreignField': 'S_SUPPKEY',
            'as': 'supplier_info'
        }
    },
    {
        '$unwind': '$supplier_info'
    },
    {
        '$lookup': {
            'from': 'customer',
            'localField': 'L_ORDERKEY',
            'foreignField': 'C_CUSTKEY',
            'as': 'customer_info'
        }
    },
    {
        '$unwind': '$customer_info'
    },
    {
        '$lookup': {
            'from': 'nation',
            'localField': 'supplier_info.S_NATIONKEY',
            'foreignField': 'N_NATIONKEY',
            'as': 'supplier_nation'
        }
    },
    {
        '$unwind': '$supplier_nation'
    },
    {
        '$lookup': {
            'from': 'nation',
            'localField': 'customer_info.C_NATIONKEY',
            'foreignField': 'N_NATIONKEY',
            'as': 'customer_nation'
        }
    },
    {
        '$unwind': '$customer_nation'
    },
    {
        '$match': {
            'L_SHIPDATE': {'$gte': datetime(START_YEAR, 1, 1), '$lte': datetime(END_YEAR, 12, 31)},
            '$or': [
                {'$and': [{'supplier_nation.N_NAME': INDIA}, {'customer_nation.N_NAME': JAPAN}]},
                {'$and': [{'supplier_nation.N_NAME': JAPAN}, {'customer_nation.N_NAME': INDIA}]}
            ]
        }
    },
    {
        '$project': {
            'supplier_nation': '$supplier_nation.N_NAME',
            'customer_nation': '$customer_nation.N_NAME',
            'year': {'$year': '$L_SHIPDATE'},
            'revenue': {
                '$multiply': [
                    '$L_EXTENDEDPRICE',
                    {'$subtract': [1, '$L_DISCOUNT']}
                ]
            }
        }
    },
    {
        '$group': {
            '_id': {
                'supplier_nation': '$supplier_nation',
                'customer_nation': '$customer_nation',
                'year': '$year'
            },
            'revenue': {'$sum': '$revenue'}
        }
    },
    {
        '$sort': {
            '_id.supplier_nation': 1,
            '_id.customer_nation': 1,
            '_id.year': 1
        }
    },
    {
        '$project': {
            '_id': 0,
            'supplier_nation': '$_id.supplier_nation',
            'customer_nation': '$_id.customer_nation',
            'year': '$_id.year',
            'revenue': '$revenue'
        }
    }
]

# Run the aggregation query
results = db.lineitem.aggregate(pipeline)

# Output to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['supplier_nation', 'customer_nation', 'year', 'revenue'])
    writer.writeheader()
    for doc in results:
        writer.writerow(doc)
