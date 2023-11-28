# suppliers_query.py
import csv
from datetime import datetime
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)

# Aggregation pipeline for the revenue0 CTE equivalent
pipeline = [
    {"$match": {
        "L_SHIPDATE": {"$gte": start_date, "$lte": end_date}
    }},
    {"$group": {
        "_id": "$L_SUPPKEY",
        "total_revenue": {"$sum": {
            "$multiply": [
                {"$subtract": [100, "$L_DISCOUNT"]},
                "$L_EXTENDEDPRICE"
            ]
        }}
    }}
]

# Aggregate lineitem table to find the revenue
revenue0 = list(db.lineitem.aggregate(pipeline))

# Find the supplier with maximum total revenue
max_revenue_supplier = max(revenue0, key=lambda x: x['total_revenue'])

# Find the supplier details
supplier_details = db.supplier.find_one({"S_SUPPKEY": max_revenue_supplier['_id']})

# Combine data to be written to CSV
output_data = {
    "S_SUPPKEY": supplier_details['S_SUPPKEY'],
    "S_NAME": supplier_details['S_NAME'],
    "S_ADDRESS": supplier_details['S_ADDRESS'],
    "S_PHONE": supplier_details['S_PHONE'],
    "Total_Revenue": max_revenue_supplier['total_revenue']
}

# Write output to CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.DictWriter(file, fieldnames=output_data.keys())
    writer.writeheader()
    writer.writerow(output_data)

# Disconnect from MongoDB
client.close()
