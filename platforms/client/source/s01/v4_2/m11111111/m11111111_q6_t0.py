import pymongo
from datetime import datetime
import csv

# Setup mongodb connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]
col = db["lineitem"]

# Filter conditions
start_date = datetime.strptime('1994-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-01-01', '%Y-%m-%d')
min_discount = .06 - 0.01
max_discount = .06 + 0.01
max_quantity = 24

# Query
results=[]
for doc in col.find({"L_SHIPDATE": {"$gte": start_date, "$lt": end_date}, "L_DISCOUNT": {"$gte": min_discount, "$lte": max_discount}, "L_QUANTITY": {"$lt": max_quantity}}):
    revenue = doc["L_EXTENDEDPRICE"] * doc["L_DISCOUNT"]
    results.append({"REVENUE": revenue})

# Write to CSV
keys = results[0].keys()
with open('query_output.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(results)
