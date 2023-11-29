# query.py

from pymongo import MongoClient
import csv
from datetime import datetime

# Establish a connection to the MongoDB server
client = MongoClient("mongodb", 27017)

# Select the database
db = client["tpch"]

# Define the pipeline for aggregation
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {"$lt": datetime(1998, 9, 2)}
        }
    },
    {
        "$group": {
            "_id": {
                "L_RETURNFLAG": "$L_RETURNFLAG",
                "L_LINESTATUS": "$L_LINESTATUS"
            },
            "sum_qty": {"$sum": "$L_QUANTITY"},
            "sum_base_price": {"$sum": "$L_EXTENDEDPRICE"},
            "sum_disc_price": {
                "$sum": {
                    "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]
                }
            },
            "sum_charge": {
                "$sum": {
                    "$multiply": [
                        {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]},
                        {"$add": [1, "$L_TAX"]}
                    ]
                }
            },
            "avg_qty": {"$avg": "$L_QUANTITY"},
            "avg_price": {"$avg": "$L_EXTENDEDPRICE"},
            "avg_disc": {"$avg": "$L_DISCOUNT"},
            "count_order": {"$sum": 1}
        }
    },
    {
        "$sort": {
            "_id.L_RETURNFLAG": 1,
            "_id.L_LINESTATUS": 1
        }
    }
]

# Run the aggregation query
results = db.lineitem.aggregate(pipeline)

# Field names for the CSV output
fieldnames = [
    "L_RETURNFLAG",
    "L_LINESTATUS",
    "SUM_QTY",
    "SUM_BASE_PRICE",
    "SUM_DISC_PRICE",
    "SUM_CHARGE",
    "AVG_QTY",
    "AVG_PRICE",
    "AVG_DISC",
    "COUNT_ORDER"
]

# Write the results to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow({
            "L_RETURNFLAG": result["_id"]["L_RETURNFLAG"],
            "L_LINESTATUS": result["_id"]["L_LINESTATUS"],
            "SUM_QTY": result["sum_qty"],
            "SUM_BASE_PRICE": result["sum_base_price"],
            "SUM_DISC_PRICE": result["sum_disc_price"],
            "SUM_CHARGE": result["sum_charge"],
            "AVG_QTY": result["avg_qty"],
            "AVG_PRICE": result["avg_price"],
            "AVG_DISC": result["avg_disc"],
            "COUNT_ORDER": result["count_order"]
        })

# Close the MongoDB connection
client.close()
