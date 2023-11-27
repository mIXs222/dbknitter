from pymongo import MongoClient
import csv
from datetime import datetime

# Establish connection to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch
lineitem_collection = db.lineitem

# Define the query using aggregation framework
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {
                "$lt": datetime(1998, 9, 2)
            }
        }
    },
    {
        "$group": {
            "_id": {
                "L_RETURNFLAG": "$L_RETURNFLAG",
                "L_LINESTATUS": "$L_LINESTATUS"
            },
            "SUM_QTY": {"$sum": "$L_QUANTITY"},
            "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
            "SUM_DISC_PRICE": {
                "$sum": {
                    "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]
                }
            },
            "SUM_CHARGE": {
                "$sum": {
                    "$multiply": [
                        {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]},
                        {"$add": [1, "$L_TAX"]}
                    ]
                }
            },
            "AVG_QTY": {"$avg": "$L_QUANTITY"},
            "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
            "AVG_DISC": {"$avg": "$L_DISCOUNT"},
            "COUNT_ORDER": {"$sum": 1}
        }
    },
    {
        "$sort": {
            "_id.L_RETURNFLAG": 1,
            "_id.L_LINESTATUS": 1
        }
    }
]

# Execute the query
result = lineitem_collection.aggregate(pipeline)

# Write result to CSV
with open('query_output.csv', mode='w') as csv_file:
    fieldnames = ['L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for doc in result:
        row = {
            'L_RETURNFLAG': doc['_id']['L_RETURNFLAG'],
            'L_LINESTATUS': doc['_id']['L_LINESTATUS'],
            'SUM_QTY': doc['SUM_QTY'],
            'SUM_BASE_PRICE': doc['SUM_BASE_PRICE'],
            'SUM_DISC_PRICE': doc['SUM_DISC_PRICE'],
            'SUM_CHARGE': doc['SUM_CHARGE'],
            'AVG_QTY': round(doc['AVG_QTY'], 2),
            'AVG_PRICE': round(doc['AVG_PRICE'], 2),
            'AVG_DISC': round(doc['AVG_DISC'], 2),
            'COUNT_ORDER': doc['COUNT_ORDER']
        }
        writer.writerow(row)

# Close the client connection
client.close()
