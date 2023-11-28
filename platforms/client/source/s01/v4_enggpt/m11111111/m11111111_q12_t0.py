# File: query_mongodb.py
import pymongo
import csv
from datetime import datetime

# Connect to the MongoDB server
client = pymongo.MongoClient('mongodb', 27017)
db = client.tpch

# Define the filters for dates
start_date = datetime(1994, 1, 1)
end_date = datetime(1994, 12, 31)

# Define the aggregation pipeline
pipeline = [
    {
        "$lookup": {
            "from": "lineitem",
            "localField": "O_ORDERKEY",
            "foreignField": "L_ORDERKEY",
            "as": "lineitems"
        }
    },
    { "$unwind": "$lineitems" },
    {
        "$match": {
            "lineitems.L_SHIPMODE": {"$in": ["MAIL", "SHIP"]},
            "lineitems.L_COMMITDATE": {"$lt": "$lineitems.L_RECEIPTDATE"},
            "lineitems.L_SHIPDATE": {"$lt": "$lineitems.L_COMMITDATE"},
            "lineitems.L_RECEIPTDATE": {"$gte": start_date, "$lte": end_date}
        }
    },
    {
        "$project": {
            "O_ORDERPRIORITY": 1,
            "L_SHIPMODE": "$lineitems.L_SHIPMODE",
            "is_high_priority": {
                "$in": ["$O_ORDERPRIORITY", ["1-URGENT", "2-HIGH"]]
            }
        }
    },
    {
        "$group": {
            "_id": {
                "L_SHIPMODE": "$L_SHIPMODE",
                "is_high_priority": "$is_high_priority"
            },
            "count": {"$sum": 1}
        }
    },
    {
        "$project": {
            "shipping_mode": "$_id.L_SHIPMODE",
            "priority": {
                "$cond": {"if": "$_id.is_high_priority", "then": "HIGH", "else": "LOW"}
            },
            "line_count": "$count",
            "_id": 0
        }
    },
    {
        "$sort": {
            "shipping_mode": 1,
            "priority": 1
        }
    }
]

# Execute the aggregation
results = list(db.orders.aggregate(pipeline))

# Transform results into the required format
output = []
for result in results:
    priority = 'HIGH_LINE_COUNT' if result['priority'] == 'HIGH' else 'LOW_LINE_COUNT'
    output.append({
        'L_SHIPMODE': result['shipping_mode'],
        priority: result['line_count']
    })

# Merge high and low priorities
final_output = {}
for item in output:
    mode = item['L_SHIPMODE']
    if mode not in final_output:
        final_output[mode] = {'HIGH_LINE_COUNT': 0, 'LOW_LINE_COUNT': 0}
    if 'HIGH_LINE_COUNT' in item:
        final_output[mode]['HIGH_LINE_COUNT'] += item['HIGH_LINE_COUNT']
    if 'LOW_LINE_COUNT' in item:
        final_output[mode]['LOW_LINE_COUNT'] += item['LOW_LINE_COUNT']

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for ship_mode, counts in final_output.items():
        writer.writerow({
            'L_SHIPMODE': ship_mode,
            'HIGH_LINE_COUNT': counts['HIGH_LINE_COUNT'],
            'LOW_LINE_COUNT': counts['LOW_LINE_COUNT']
        })
