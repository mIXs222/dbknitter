from pymongo import MongoClient
import csv
from datetime import datetime

# MongoDB connection details
HOST = "mongodb"
PORT = 27017
DATABASE = "tpch"

# Create MongoDB client and connect to the database
client = MongoClient(HOST, PORT)
db = client[DATABASE]

# Filter to find line items with a shipping date on or before September 2, 1998
date_filter = {
    "L_SHIPDATE": {"$lte": datetime(1998, 9, 2)}
}

# Perform the aggregation
pipeline = [
    {"$match": date_filter},
    {"$group": {
        "_id": {
            "L_RETURNFLAG": "$L_RETURNFLAG",
            "L_LINESTATUS": "$L_LINESTATUS"
        },
        "SUM_QTY": {"$sum": "$L_QUANTITY"},
        "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
        "SUM_DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}] }},
        "SUM_CHARGE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}, {"$add": [1, "$L_TAX"]}] }},
        "AVG_QTY": {"$avg": "$L_QUANTITY"},
        "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
        "AVG_DISC": {"$avg": "$L_DISCOUNT"},
        "COUNT_ORDER": {"$sum": 1}
    }},
    {"$sort": {"_id.L_RETURNFLAG": 1, "_id.L_LINESTATUS": 1}}
]

# Execute the aggregation query
results = list(db.lineitem.aggregate(pipeline))

# Write the results to a CSV file
with open("query_output.csv", "w", newline='') as csv_file:
    fieldnames = [
        'RETURN_FLAG',
        'LINE_STATUS',
        'SUM_QTY',
        'SUM_BASE_PRICE',
        'SUM_DISC_PRICE',
        'SUM_CHARGE',
        'AVG_QTY',
        'AVG_PRICE',
        'AVG_DISC',
        'COUNT_ORDER'
    ]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow({
            'RETURN_FLAG': result['_id']['L_RETURNFLAG'],
            'LINE_STATUS': result['_id']['L_LINESTATUS'],
            'SUM_QTY': result['SUM_QTY'],
            'SUM_BASE_PRICE': result['SUM_BASE_PRICE'],
            'SUM_DISC_PRICE': result['SUM_DISC_PRICE'],
            'SUM_CHARGE': result['SUM_CHARGE'],
            'AVG_QTY': result['AVG_QTY'],
            'AVG_PRICE': result['AVG_PRICE'],
            'AVG_DISC': result['AVG_DISC'],
            'COUNT_ORDER': result['COUNT_ORDER']
        })
