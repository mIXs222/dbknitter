from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient("mongodb", 27017)
db = client["tpch"]
lineitem_collection = db["lineitem"]

# Define the query
query = {
    "L_SHIPDATE": {"$gte": "1994-01-01", "$lte": "1994-12-31"},
    "L_DISCOUNT": {"$gte": 0.05, "$lte": 0.07},
    "L_QUANTITY": {"$lt": 24},
}

# Execute the query and calculate revenue
total_revenue = 0
lineitems = lineitem_collection.find(query)
for lineitem in lineitems:
    total_revenue += lineitem["L_EXTENDEDPRICE"] * (1 - lineitem["L_DISCOUNT"])

# Write the output to a file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Total Revenue'])
    writer.writerow([total_revenue])
