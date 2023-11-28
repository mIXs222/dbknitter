from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to the MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Calculate the total revenue for suppliers within the date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': start_date,
                '$lte': end_date
            }
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'total_revenue': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE',
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        }
    },
    {'$sort': {'total_revenue': -1}}
]

lineitems = db.lineitem.aggregate(pipeline)

# Retrieve supplier with the maximum total revenue
max_revenue_supplier = next(lineitems)

# Get details of the supplier with the maximum revenue
supplier_details = db.supplier.find_one(
    {'S_SUPPKEY': max_revenue_supplier['_id']},
    {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_PHONE': 1}
)
supplier_details['total_revenue'] = max_revenue_supplier['total_revenue']

# Write the results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    headers = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'total_revenue']
    writer.writerow(headers)
    writer.writerow([supplier_details['S_SUPPKEY'], supplier_details['S_NAME'],
                     supplier_details['S_ADDRESS'], supplier_details['S_PHONE'],
                     supplier_details['total_revenue']])
