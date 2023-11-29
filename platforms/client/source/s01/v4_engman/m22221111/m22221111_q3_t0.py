from pymongo import MongoClient
import csv
import os

# MongoDB connection setup
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Perform the query
query_result = db.orders.aggregate([
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "lineitems"
        }
    },
    {
        "$lookup": {
            "from": "customer",
            "localField": "O_CUSTKEY",
            "foreignField": "C_CUSTKEY",
            "as": "customer"
        }
    },
    {"$unwind": "$lineitems"},
    {"$unwind": "$customer"},
    {
        "$match": {
            "O_ORDERDATE": {"$lt": "1995-03-05"},
            "lineitems.L_SHIPDATE": {"$gt": "1995-03-15"},
            "customer.C_MKTSEGMENT": "BUILDING"
        }
    },
    {
        "$project": {
            "O_ORDERKEY": 1,
            "REVENUE": {"$multiply": ["$lineitems.L_EXTENDEDPRICE", {"$subtract": [1, "$lineitems.L_DISCOUNT"]}]},
            "O_ORDERDATE": 1,
            "O_SHIPPRIORITY": 1
        }
    },
    {"$sort": {"REVENUE": -1}}
])

# Write query output to CSV file
output_file = 'query_output.csv'
with open(output_file, 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for document in query_result:
        row = {field: document.get(field) for field in fieldnames}
        writer.writerow(row)

print(f"Query results have been written to {output_file}")

# Close the MongoDB client
client.close()
