from pymongo import MongoClient
import csv

# Connect to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Perform the query equivalent
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': '1994-01-01',
                '$lt': '1995-01-01'
            },
            'L_DISCOUNT': {
                '$gte': 0.05,
                '$lte': 0.07
            },
            'L_QUANTITY': {
                '$lt': 24
            }
        }
    },
    {
        '$project': {
            'REVENUE': {
                '$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']
            }
        }
    },
    {
        '$group': {
            '_id': None,
            'TOTAL_REVENUE': {'$sum': '$REVENUE'}
        }
    }
]

result = list(lineitem_collection.aggregate(pipeline))

# Write the output to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['REVENUE'])
    for data in result:
        writer.writerow([data['TOTAL_REVENUE']])
