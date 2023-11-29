from pymongo import MongoClient
import csv

# Setup MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Define the query filters
filters = {
    'L_SHIPDATE': {'$gt': '1994-01-01', '$lt': '1995-01-01'},
    'L_DISCOUNT': {'$gte': 0.05, '$lte': 0.07},
    'L_QUANTITY': {'$lt': 24},
}

# Perform the aggregation
pipeline = [
    {'$match': filters},
    {'$project': {
        'revenue': {'$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']}
    }},
    {'$group': {
        '_id': None,
        'REVENUE': {'$sum': '$revenue'}
    }},
    {'$project': {
        '_id': 0,
        'REVENUE': 1
    }}
]

result = list(lineitem_collection.aggregate(pipeline))

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['REVENUE'])
    writer.writeheader()
    for data in result:
        writer.writerow(data)
