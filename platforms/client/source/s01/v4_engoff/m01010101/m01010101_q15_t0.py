# top_supplier_query.py

from pymongo import MongoClient
import csv
from datetime import datetime

# MongoDB connection information
DATABASE_NAME = 'tpch'
PORT = 27017
HOSTNAME = 'mongodb'

# Establish a connection to the MongoDB server
client = MongoClient(HOSTNAME, PORT)
db = client[DATABASE_NAME]

# Define the date range for filtering
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)

# Query lineitem collection for revenue within the date range
pipeline = [
    {"$match": {
        "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
    }},
    {"$group": {
        "_id": "$L_SUPPKEY",
        "total_revenue": {"$sum": {
            "$multiply": [
                "$L_EXTENDEDPRICE",
                {"$subtract": [1, "$L_DISCOUNT"]}
            ]
        }}
    }},
    {"$sort": {
        "total_revenue": -1,
        "_id": 1
    }}
]

# Execute aggregation pipeline
lineitem_results = db.lineitem.aggregate(pipeline)

# Determine the maximum revenue from the results
max_revenue = None
top_suppliers = []
for result in lineitem_results:
    if max_revenue is None or result['total_revenue'] == max_revenue:
        max_revenue = result['total_revenue']
        top_suppliers.append(result['_id'])
    elif result['total_revenue'] < max_revenue:
        break

# Query supplier collection for supplier details using the top_suppliers list
top_supplier_details = list(db.supplier.find({"S_SUPPKEY": {"$in": top_suppliers}}))

# Write query results to 'query_output.csv'
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])

    for supplier in top_supplier_details:
        writer.writerow([
            supplier['S_SUPPKEY'],
            supplier['S_NAME'],
            supplier['S_ADDRESS'],
            supplier['S_NATIONKEY'],
            supplier['S_PHONE'],
            supplier['S_ACCTBAL'],
            supplier['S_COMMENT']
        ])

print("Query results have been written to query_output.csv")
