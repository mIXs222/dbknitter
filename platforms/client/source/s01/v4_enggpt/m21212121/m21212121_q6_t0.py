from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Define the range for discounts and quantity
discount_range = (0.05, 0.07)
quantity_limit = 24
# Define the date range for shipping
start_date = datetime(1994, 1, 1)
end_date = datetime(1994, 12, 31)

# Define the query
query = {
    'L_DISCOUNT': {'$gte': discount_range[0], '$lte': discount_range[1]},
    'L_QUANTITY': {'$lt': quantity_limit},
    'L_SHIPDATE': {'$gte': start_date, '$lte': end_date}
}

# Aggregate the results
pipeline = [
    {'$match': query},
    {'$project': {
        'revenue': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}
    }},
    {'$group': {
        '_id': None,
        'total_revenue': {'$sum': '$revenue'}
    }}
]

# Execute the aggregation
total_revenue = 0
try:
    cursor = lineitem_collection.aggregate(pipeline)
    for data in cursor:
        total_revenue = data['total_revenue']
finally:
    client.close()

# Write the results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['total_revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({'total_revenue': total_revenue})
