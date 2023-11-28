# mongo_query.py

from datetime import datetime
from pymongo import MongoClient
import csv

# Establish a connection to the MongoDB server
client = MongoClient("mongodb", 27017)
db = client["tpch"]

# Define the query parameters
start_date = datetime(1994, 1, 1)
end_date = datetime(1994, 12, 31)
min_discount = 0.06 - 0.01
max_discount = 0.06 + 0.01
max_quantity = 24

# Construct the query with the specified criteria
query = {
    "L_SHIPDATE": {"$gte": start_date, "$lte": end_date},
    "L_DISCOUNT": {"$gte": min_discount, "$lte": max_discount},
    "L_QUANTITY": {"$lt": max_quantity},
}

# Perform the aggregation to calculate total revenue
pipeline = [
    {"$match": query},
    {"$project": {
        "revenue": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}
    }},
    {"$group": {
        "_id": None,
        "total_revenue": {"$sum": "$revenue"}
    }}
]

result = db.lineitem.aggregate(pipeline)

# Output the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['total_revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in result:
        writer.writerow({'total_revenue': data['total_revenue'] if data['total_revenue'] else 0})

client.close()
