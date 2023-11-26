# revenue_from_mongodb.py
from pymongo import MongoClient
import csv

# Function to connect to MongoDB
def get_mongodb_connection(host='mongodb', port=27017, db_name='tpch'):
    client = MongoClient(host, port)
    return client[db_name]

# Connect to the MongoDB database
db = get_mongodb_connection()

# Perform the query equivalent on MongoDB
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': '1994-01-01',
                '$lt': '1995-01-01'
            },
            'L_DISCOUNT': {
                '$gte': .06 - 0.01,
                '$lte': .06 + 0.01
            },
            'L_QUANTITY': { '$lt': 24 }
        }
    },
    {
        '$group': {
            '_id': None,
            'REVENUE': {
                '$sum': {
                    '$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']
                }
            }
        }
    }
]

result = list(db.lineitem.aggregate(pipeline))

# Write the output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    if result:
        writer.writerow({'REVENUE': result[0]['REVENUE']})
