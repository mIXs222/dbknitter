import pymongo
import csv
from datetime import datetime

# Connect to the MongoDB
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Build the query
start_date = datetime.strptime('1994-01-02', '%Y-%m-%d')
end_date = datetime.strptime('1995-01-01', '%Y-%m-%d')
min_discount = 0.06 - 0.01
max_discount = 0.06 + 0.01

pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gt': start_date, '$lt': end_date},
            'L_DISCOUNT': {'$gte': min_discount, '$lte': max_discount},
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
    },
    {
        '$project': {
            'REVENUE': '$TOTAL_REVENUE',
            '_id': 0
        }
    }
]

# Execute the query
results = list(lineitem_collection.aggregate(pipeline))

# Write results to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['REVENUE'])
    writer.writeheader()
    if results:
        writer.writerow(results[0])

# Close the client connection
client.close()
