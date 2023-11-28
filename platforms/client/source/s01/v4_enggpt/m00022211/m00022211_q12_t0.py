from pymongo import MongoClient
import csv
from datetime import datetime

# MongoDB connection
client = MongoClient('mongodb', 27017)
db = client.tpch

# Define the priority categories for filtering
high_priority = ['1-URGENT', '2-HIGH']

# Query MongoDB for orders and join with lineitem collection
pipeline = [
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "lineitems"
        }
    },
    {
        "$unwind": "$lineitems"
    },
    {
        "$match": {
            "O_ORDERDATE": {"$gte": datetime(1994, 1, 1), "$lte": datetime(1994, 12, 31)},
            "lineitems.L_COMMITDATE": {"$lt": "$lineitems.L_RECEIPTDATE"},
            "lineitems.L_SHIPDATE": {"$lt": "$lineitems.L_COMMITDATE"},
            "lineitems.L_SHIPMODE": {"$in": ["MAIL", "SHIP"]}
        }
    },
    {
        "$group": {
            "_id": {"shipping_mode": "$lineitems.L_SHIPMODE", "high_priority": {"$in": ["$O_ORDERPRIORITY", high_priority]}},
            "line_count": {"$sum": 1}
        }
    },
    {
        "$project": {
            "shipping_mode": "$_id.shipping_mode",
            "high_priority": "$_id.high_priority",
            "line_count": "$line_count",
            "_id": 0
        }
    },
    {
        "$sort": {"shipping_mode": 1}
    }
]
lines_by_shipmode_priority = list(db.orders.aggregate(pipeline))

# Process results and write to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['shipping_mode', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])

    # Initialize counts
    results = {mode: {'HIGH_LINE_COUNT': 0, 'LOW_LINE_COUNT': 0} for mode in ['MAIL', 'SHIP']}
    
    # Iterate through the results and calculate the counts
    for entry in lines_by_shipmode_priority:
        mode = entry['shipping_mode']
        priority = "HIGH_LINE_COUNT" if entry['high_priority'] else "LOW_LINE_COUNT"
        results[mode][priority] += entry['line_count']

    # Write the results
    for mode in sorted(results):
        writer.writerow([mode, results[mode]['HIGH_LINE_COUNT'], results[mode]['LOW_LINE_COUNT']])

client.close()
