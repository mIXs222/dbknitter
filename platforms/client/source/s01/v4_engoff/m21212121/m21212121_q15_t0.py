# top_supplier_query.py
import csv
from datetime import datetime
from pymongo import MongoClient

# Connect to the MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# MongoDB query to find the top supplier
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': start_date, '$lte': end_date}
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'total_revenue': {'$sum': {'$multiply': ['$L_EXTENDEDPRICE', {'$subtract': [1, '$L_DISCOUNT']}]}}
        }
    },
    {
        '$sort': {
            'total_revenue': -1,
            '_id': 1
        }
    },
    {
        '$limit': 1  # Adjust this if you want more top suppliers in case of a tie
    }
]

# Execute the query
top_suppliers = list(db.lineitem.aggregate(pipeline))

# Join with supplier table
top_suppliers_info = []
for supplier in top_suppliers:
    supplier_info = db.supplier.find_one({'S_SUPPKEY': supplier['_id']}, {'_id': 0})
    if supplier_info:
        supplier_info['total_revenue'] = supplier['total_revenue']
        top_suppliers_info.append(supplier_info)

# Write query output to csv
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT', 'total_revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for row in top_suppliers_info:
        writer.writerow(row)
