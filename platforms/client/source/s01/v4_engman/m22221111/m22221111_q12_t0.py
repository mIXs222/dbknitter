import pymongo
import csv
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client.tpch

# Query parameters
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
ships = ["MAIL", "SHIP"]

# Aggregation pipeline
pipeline = [
    {
        "$lookup": {
            "from": "orders",
            "localField": "L_ORDERKEY",
            "foreignField": "O_ORDERKEY",
            "as": "order"
        }
    },
    {"$unwind": "$order"},
    {
        "$match": {
            "L_RECEIPTDATE": {"$gte": start_date, "$lt": end_date},
            "L_COMMITDATE": {"$lt": "$L_RECEIPTDATE"},
            "L_SHIPDATE": {"$lt": "$L_COMMITDATE"},
            "L_SHIPMODE": {"$in": ships}
        }
    },
    {
        "$project": {
            "L_SHIPMODE": 1,
            "high_priority_count": {
                "$cond": [{"$in": ["$order.O_ORDERPRIORITY", ["URGENT", "HIGH"]]}, 1, 0]
            },
            "low_priority_count": {
                "$cond": [{"$not": {"$in": ["$order.O_ORDERPRIORITY", ["URGENT", "HIGH"]]}}, 1, 0]
            }
        }
    },
    {
        "$group": {
            "_id": "$L_SHIPMODE",
            "high_priority_count": {"$sum": "$high_priority_count"},
            "low_priority_count": {"$sum": "$low_priority_count"}
        }
    },
    {"$sort": {"_id": 1}} # Sorting by L_SHIPMODE in ascending order
]

results = db.lineitem.aggregate(pipeline)

# Output results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_SHIPMODE', 'high_priority_count', 'low_priority_count']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow({
            'L_SHIPMODE': result['_id'],
            'high_priority_count': result['high_priority_count'],
            'low_priority_count': result['low_priority_count']
        })

# Close the MongoDB connection
client.close()
