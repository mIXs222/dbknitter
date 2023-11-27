import csv
from pymongo import MongoClient
from datetime import datetime

# Connect to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Criteria for querying the database
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
min_discount = 0.06 - 0.01
max_discount = 0.06 + 0.01
max_quantity = 24

# Query to find all relevant lineitems and calculate the potential revenue increase
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': start_date, '$lt': end_date},
            'L_DISCOUNT': {'$gte': min_discount, '$lte': max_discount},
            'L_QUANTITY': {'$lt': max_quantity}
        }
    },
    {
        '$project': {
            "revenue_increase": {
                '$multiply': ['$L_EXTENDEDPRICE', '$L_DISCOUNT']
            }
        }
    },
    {
        '$group': {
            '_id': None,
            'total_revenue_increase': {'$sum': '$revenue_increase'}
        }
    }
]

# Execute the query
result = db.lineitem.aggregate(pipeline)

# Extract the result and write it to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['total_revenue_increase']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for data in result:
        writer.writerow({'total_revenue_increase': data['total_revenue_increase']})
