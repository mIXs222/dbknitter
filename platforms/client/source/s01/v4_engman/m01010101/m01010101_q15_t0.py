from pymongo import MongoClient
import csv
from datetime import datetime

# Constants
MONGODB_HOST = 'mongodb'
MONGODB_PORT = 27017
MONGODB_DB_NAME = 'tpch'

# MongoDB connection
client = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
db = client[MONGODB_DB_NAME]

# Query to find suppliers contribution in the given date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)

pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'TOTAL_REVENUE': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE',
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        }
    },
    {
        '$sort': {'TOTAL_REVENUE': -1, '_id': 1}
    },
    {
        '$lookup': {
            'from': 'supplier',
            'localField': '_id',
            'foreignField': 'S_SUPPKEY',
            'as': 'supplier_info'
        }
    },
    {
        '$unwind': '$supplier_info'
    },
    {
        '$project': {
            'S_SUPPKEY': '$supplier_info.S_SUPPKEY',
            'S_NAME': '$supplier_info.S_NAME',
            'S_ADDRESS': '$supplier_info.S_ADDRESS',
            'S_PHONE': '$supplier_info.S_PHONE',
            'TOTAL_REVENUE': 1
        }
    }
]

# Find the top total revenue
top_revenue = None
results = []

# Apply pipeline
for doc in db.lineitem.aggregate(pipeline):
    revenue = doc['TOTAL_REVENUE']
    if top_revenue is None or revenue >= top_revenue:
        top_revenue = revenue
        results.append(doc)
    else:
        break  # No need to continue as we have found the top revenue

# Write result to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for item in results:
        writer.writerow({
            'S_SUPPKEY': item['S_SUPPKEY'],
            'S_NAME': item['S_NAME'],
            'S_ADDRESS': item['S_ADDRESS'],
            'S_PHONE': item['S_PHONE'],
            'TOTAL_REVENUE': item['TOTAL_REVENUE']
        })
