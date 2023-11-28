from pymongo import MongoClient
import csv
from datetime import datetime

# Connecting to the MongoDB server
client = MongoClient("mongodb", 27017)
db = client.tpch
lineitem_collection = db.lineitem

# Query to match the criteria
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': datetime(1994, 1, 1),
                '$lte': datetime(1994, 12, 31)
            },
            'L_DISCOUNT': {'$gte': 0.05, '$lte': 0.07},
            'L_QUANTITY': {'$lt': 24}
        }
    },
    {
        '$group': {
            '_id': None,
            'total_revenue': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE', 
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        }
    }
]

# Executing the aggregation pipeline
result = list(lineitem_collection.aggregate(pipeline))

# Outputting the results to CSV
output_file = 'query_output.csv'
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['total_revenue'])  # header
    if result and 'total_revenue' in result[0]:
        writer.writerow([result[0]['total_revenue']])

# Close the MongoDB client
client.close()
