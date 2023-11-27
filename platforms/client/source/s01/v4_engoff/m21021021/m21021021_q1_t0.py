from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to the MongoDB database
client = MongoClient('mongodb', 27017)
db = client.tpch

# Define the pipeline for the aggregation
pipeline = [
    {"$match": {
        "L_SHIPDATE": {"$lt": datetime(1998, 9, 2)}
    }},
    {"$group": {
        "_id": {
            "RETURNFLAG": "$L_RETURNFLAG",
            "LINESTATUS": "$L_LINESTATUS"
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
    {"$sort": {
        "_id.RETURNFLAG": 1,
        "_id.LINESTATUS": 1
    }}
]

# Execute the aggregation pipeline
result = db.lineitem.aggregate(pipeline)

# Write the output to 'query_output.csv'
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for r in result:
        writer.writerow({
            'RETURNFLAG': r['_id']['RETURNFLAG'],
            'LINESTATUS': r['_id']['LINESTATUS'],
            'SUM_QTY': r['SUM_QTY'],
            'SUM_BASE_PRICE': r['SUM_BASE_PRICE'],
            'SUM_DISC_PRICE': r['SUM_DISC_PRICE'],
            'SUM_CHARGE': r['SUM_CHARGE'],
            'AVG_QTY': r['AVG_QTY'],
            'AVG_PRICE': r['AVG_PRICE'],
            'AVG_DISC': r['AVG_DISC'],
            'COUNT_ORDER': r['COUNT_ORDER']
        })

# Close the MongoDB client
client.close()
