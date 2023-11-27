# mongo_query.py
from pymongo import MongoClient
import csv

# Connect to the MongoDB
client = MongoClient("mongodb", 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Perform the query
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'},
            'L_DISCOUNT': {'$gte': 0.05, '$lte': 0.07},
            'L_QUANTITY': {'$lt': 24}
        }
    },
    {
        '$project': {
            'REVENUE': {'$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']},
            '_id': 0
        }
    },
    {
        '$group': {
            '_id': None,
            'TOTAL_REVENUE': {'$sum': '$REVENUE'}
        }
    }
]

# Execute the query and get the result
result = list(lineitem_collection.aggregate(pipeline))

# Write the output to a CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(['REVENUE'])
    if result:
        writer.writerow([result[0]['TOTAL_REVENUE']])

# Close the MongoDB connection
client.close()
