from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Aggregation pipeline
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
            "SUM_QTY": {"$sum": "$L_QUANTITY"},
            "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
            "SUM_DISC_PRICE": {
                "$sum": {
                    "$multiply": [
                        "$L_EXTENDEDPRICE",
                        {"$subtract": [1, "$L_DISCOUNT"]}
                    ]
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
            "AVERAGE_QTY": {"$avg": "$L_QUANTITY"},
            "AVERAGE_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
            "AVERAGE_DISC": {"$avg": "$L_DISCOUNT"},
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

# Execute the aggregation
result = db.lineitem.aggregate(pipeline)

# Write results to csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE',
                     'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'])

    for record in result:
        writer.writerow([
            record['_id']['L_RETURNFLAG'],
            record['_id']['L_LINESTATUS'],
            record['SUM_QTY'],
            record['SUM_BASE_PRICE'],
            record['SUM_DISC_PRICE'],
            record['SUM_CHARGE'],
            record['AVERAGE_QTY'],
            record['AVERAGE_PRICE'],
            record['AVERAGE_DISC'],
            record['COUNT_ORDER']
        ])
