# query.py
from pymongo import MongoClient
import csv
import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch
lineitem_collection = db.lineitem

# Define the cutoff date for the shipping date
cutoff_date = datetime.datetime(1998, 9, 2)

# Define the aggregation pipeline for the query
pipeline = [
    {"$match": {"L_SHIPDATE": {"$lte": cutoff_date}}},
    {"$group": {
        "_id": {
            "L_RETURNFLAG": "$L_RETURNFLAG",
            "L_LINESTATUS": "$L_LINESTATUS"
        },
        "SUM_QTY": {"$sum": "$L_QUANTITY"},
        "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
        "SUM_DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}] }},
        "SUM_CHARGE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$add": [1, "$L_TAX"]}] }},
        "AVG_QTY": {"$avg": "$L_QUANTITY"},
        "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
        "AVG_DISC": {"$avg": "$L_DISCOUNT"},
        "COUNT_ORDER": {"$sum": 1}
    }},
    {"$sort": {
        "_id.L_RETURNFLAG": 1,
        "_id.L_LINESTATUS": 1
    }}
]

# Execute the aggregation
results = list(lineitem_collection.aggregate(pipeline))

# Writing results to query_output.csv
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = [
        'L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE',
        'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE',
        'AVG_DISC', 'COUNT_ORDER'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow({
            'L_RETURNFLAG': result['_id']['L_RETURNFLAG'],
            'L_LINESTATUS': result['_id']['L_LINESTATUS'],
            'SUM_QTY': result['SUM_QTY'],
            'SUM_BASE_PRICE': result['SUM_BASE_PRICE'],
            'SUM_DISC_PRICE': result['SUM_DISC_PRICE'],
            'SUM_CHARGE': result['SUM_CHARGE'],
            'AVG_QTY': result['AVG_QTY'],
            'AVG_PRICE': result['AVG_PRICE'],
            'AVG_DISC': result['AVG_DISC'],
            'COUNT_ORDER': result['COUNT_ORDER']
        })

# Close the client connection
client.close()
