from pymongo import MongoClient
import csv

# Connect to the MongoDB client
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db.lineitem

# Define the MongoDB query (using the same conditions as the SQL query)
query = {
    "L_SHIPDATE": {"$gte": "1994-01-01", "$lt": "1995-01-01"},
    "L_DISCOUNT": {"$gte": 0.06 - 0.01, "$lte": 0.06 + 0.01},
    "L_QUANTITY": {"$lt": 24},
}

# Define the project fields (to calculate the revenue)
project = {
    "_id": 0,
    "REVENUE": {"$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]},
}

# Query the lineitem collection and calculate the revenue
revenue_result = lineitem_collection.aggregate([
    {"$match": query},
    {"$project": project},
    {"$group": {"_id": None, "REVENUE": {"$sum": "$REVENUE"}}}
])

# Get the result
revenue = list(revenue_result)[0]['REVENUE'] if list(revenue_result) else None

# Write the output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['REVENUE'])
    if revenue:
        writer.writerow([revenue])
