import pymongo
from datetime import datetime
import csv

# Establish a connection to the MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Define the date range
start_date = datetime(1993, 7, 1)
end_date = datetime(1993, 10, 1)

# Query to find the valid order keys in the lineitem collection
valid_order_keys = db.lineitem.distinct("L_ORDERKEY", {
    "L_COMMITDATE": {"$lt": "$L_RECEIPTDATE"},
    "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
})

# Now find the counts of orders with those keys, grouped by O_ORDERPRIORITY
pipeline = [
    {
        "$match": {
            "O_ORDERKEY": {"$in": valid_order_keys},
            "O_ORDERDATE": {"$gte": start_date, "$lt": end_date}
        }
    },
    {
        "$group": {
            "_id": "$O_ORDERPRIORITY",
            "count": {"$sum": 1}
        }
    },
    {"$sort": {"_id": 1}}
]

# Execute the aggregation
order_priority_counts = list(db.orders.aggregate(pipeline))

# Write output to a CSV file
with open("query_output.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["order_priority", "count"])
    for doc in order_priority_counts:
        writer.writerow([doc['_id'], doc['count']])
