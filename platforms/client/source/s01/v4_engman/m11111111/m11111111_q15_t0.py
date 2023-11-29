import pymongo
import csv
from datetime import datetime

# Connect to the MongoDB server
client = pymongo.MongoClient("mongodb://mongodb:27017/")

# Define the database and collections
db = client["tpch"]
supplier_collection = db["supplier"]
lineitem_collection = db["lineitem"]

# Date range for parts shipped
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)

# Aggregate query to calculate total revenue
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
                    '$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]
                }
            }
        }
    },
    {'$sort': {'TOTAL_REVENUE': -1, '_id': 1}}
]

# Execute aggregation
revenue_result = list(lineitem_collection.aggregate(pipeline))

# Find max revenue and filter the suppliers who have this revenue
max_revenue = revenue_result[0]['TOTAL_REVENUE'] if revenue_result else 0
top_suppliers = [x['_id'] for x in revenue_result if x['TOTAL_REVENUE'] == max_revenue]

# Fetch supplier details
supplier_details = list(supplier_collection.find({'S_SUPPKEY': {'$in': top_suppliers}}))

# Prepare the output CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])

    for supplier in supplier_details:
        writer.writerow([
            supplier['S_SUPPKEY'], supplier['S_NAME'], supplier['S_ADDRESS'],
            supplier['S_PHONE'], next((item['TOTAL_REVENUE'] for item in revenue_result if item['_id'] == supplier['S_SUPPKEY']), 0)
        ])
