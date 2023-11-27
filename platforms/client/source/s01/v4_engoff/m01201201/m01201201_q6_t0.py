from pymongo import MongoClient
from datetime import datetime
import csv

# Connect to the MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Prepare the query
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
min_discount = 0.06 - 0.01
max_discount = 0.06 + 0.01

# Execute the query
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': start_date, '$lt': end_date},
            'L_DISCOUNT': {'$gte': min_discount, '$lte': max_discount},
            'L_QUANTITY': {'$lt': 24}
        }
    },
    {
        '$project': {
            'revenue_increase': {
                '$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']
            }
        }
    },
    {
        '$group': {
            '_id': None,
            'total_revenue_increase': {
                '$sum': '$revenue_increase'
            }
        }
    }
]

result = list(db.lineitem.aggregate(pipeline))

# Write output into a file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['total_revenue_increase'])

    if result and 'total_revenue_increase' in result[0]:
        writer.writerow([result[0]['total_revenue_increase']])
