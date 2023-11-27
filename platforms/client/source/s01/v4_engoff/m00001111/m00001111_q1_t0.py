from pymongo import MongoClient
import csv
from datetime import datetime

# Constants
MONGO_DB_NAME = 'tpch'
MONGO_COLLECTION_NAME = 'lineitem'
MONGO_HOST = 'mongodb'
MONGO_PORT = 27017

# Connect to MongoDB
client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client[MONGO_DB_NAME]
lineitem_collection = db[MONGO_COLLECTION_NAME]

# Query to aggregate data
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$lt': datetime(1998, 9, 2)}
        }
    },
    {
        '$group': {
            '_id': {'RETURNFLAG': '$L_RETURNFLAG', 'LINESTATUS': '$L_LINESTATUS'},
            'TOTAL_QTY': {'$sum': '$L_QUANTITY'},
            'TOTAL_BASE_PRICE': {'$sum': '$L_EXTENDEDPRICE'},
            'TOTAL_DISC_PRICE': {
                '$sum': {
                    '$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]
                }
            },
            'TOTAL_CHARGE': {
                '$sum': {
                    '$multiply': [
                        {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]},
                        {'$add': [1, '$L_TAX']}
                    ]
                }
            },
            'AVG_QTY': {'$avg': '$L_QUANTITY'},
            'AVG_PRICE': {'$avg': '$L_EXTENDEDPRICE'},
            'AVG_DISC': {'$avg': '$L_DISCOUNT'},
            'COUNT_ORDER': {'$sum': 1}
        }
    },
    {
        '$sort': {
            '_id.RETURNFLAG': 1,
            '_id.LINESTATUS': 1
        }
    }
]

# Execute the aggregate query
results = list(lineitem_collection.aggregate(pipeline))

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['RETURNFLAG', 'LINESTATUS', 'TOTAL_QTY', 'TOTAL_BASE_PRICE',
                  'TOTAL_DISC_PRICE', 'TOTAL_CHARGE', 'AVG_QTY', 'AVG_PRICE',
                  'AVG_DISC', 'COUNT_ORDER']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow({
            'RETURNFLAG': result['_id']['RETURNFLAG'],
            'LINESTATUS': result['_id']['LINESTATUS'],
            'TOTAL_QTY': result['TOTAL_QTY'],
            'TOTAL_BASE_PRICE': result['TOTAL_BASE_PRICE'],
            'TOTAL_DISC_PRICE': result['TOTAL_DISC_PRICE'],
            'TOTAL_CHARGE': result['TOTAL_CHARGE'],
            'AVG_QTY': result['AVG_QTY'],
            'AVG_PRICE': result['AVG_PRICE'],
            'AVG_DISC': result['AVG_DISC'],
            'COUNT_ORDER': result['COUNT_ORDER']
        })
