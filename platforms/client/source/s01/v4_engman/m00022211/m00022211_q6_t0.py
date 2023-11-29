from pymongo import MongoClient
import csv
from datetime import datetime

# MongoDB connection setup
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem = db['lineitem']

# Query conditions
start_date = datetime(1994, 1, 2)
end_date = datetime(1995, 1, 1)
min_discount = 0.05
max_discount = 0.07
max_quantity = 24

# Query execution
query_result = lineitem.aggregate([
    {
        "$match": {
            "L_SHIPDATE": {"$gte": start_date, "$lt": end_date},
            "L_DISCOUNT": {"$gte": min_discount, "$lte": max_discount},
            "L_QUANTITY": {"$lt": max_quantity}
        }
    },
    {
        "$project": {
            "REVENUE": {"$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]},
            "_id": 0
        }
    },
    {
        "$group": {
            "_id": None,
            "TOTAL_REVENUE": {"$sum": "$REVENUE"}
        }
    },
    {
        "$project": {
            "REVENUE": "$TOTAL_REVENUE",
            "_id": 0
        }
    }
])

# Writing to CSV
with open('query_output.csv', mode='w', newline='') as csvfile:
    fieldnames = ['REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for document in query_result:
        writer.writerow(document)

# Close MongoDB connection
client.close()
