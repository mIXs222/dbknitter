from pymongo import MongoClient
from datetime import datetime
import csv

# Connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# MongoDB query
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
            "O_ORDERDATE": {
                "$gte": datetime(1994, 1, 1),
                "$lte": datetime(1994, 12, 31),
            },
            "lineitems.L_SHIPMODE": {"$in": ["MAIL", "SHIP"]},
            "lineitems.L_COMMITDATE": {"$lt": "lineitems.L_RECEIPTDATE"},
            "lineitems.L_SHIPDATE": {"$lt": "lineitems.L_COMMITDATE"}
        }
    },
    {
        "$project": {
            "ship_mode": "$lineitems.L_SHIPMODE",
            "priority": {
                "$cond": {
                    "if": {
                        "$or": [
                            {"$eq": ["$O_ORDERPRIORITY", "1-URGENT"]},
                            {"$eq": ["$O_ORDERPRIORITY", "2-HIGH"]}
                        ]},
                    "then": "HIGH_LINE_COUNT",
                    "else": "LOW_LINE_COUNT"
                }
            }
        }
    },
    {
        "$group": {
            "_id": {
                "ship_mode": "$ship_mode",
                "priority": "$priority"
            },
            "count": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id.ship_mode": 1}
    }
]

# Execute aggregation pipeline
results = db['orders'].aggregate(pipeline)

# Process results and write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['SHIP_MODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    current_ship_mode = None
    high_count = low_count = 0
    
    for result in results:
        ship_mode = result["_id"]["ship_mode"]
        if current_ship_mode != ship_mode:
            if current_ship_mode is not None:
                writer.writerow({
                    'SHIP_MODE': current_ship_mode,
                    'HIGH_LINE_COUNT': high_count,
                    'LOW_LINE_COUNT': low_count
                })
            current_ship_mode = ship_mode
            high_count = low_count = 0

        if result["_id"]["priority"] == 'HIGH_LINE_COUNT':
            high_count = result["count"]
        else:
            low_count = result["count"]

    if current_ship_mode is not None:
        writer.writerow({
            'SHIP_MODE': current_ship_mode,
            'HIGH_LINE_COUNT': high_count,
            'LOW_LINE_COUNT': low_count
        })
