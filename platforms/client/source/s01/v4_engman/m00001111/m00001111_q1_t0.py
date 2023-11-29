from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to the MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the query for MongoDB
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
            "SUM_DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}},
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
        "$sort": {"_id.L_RETURNFLAG": 1, "_id.L_LINESTATUS": 1}
    }
]

# Execute the query
result = db.lineitem.aggregate(pipeline)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE',
                  'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for document in result:
        row = {
            'L_RETURNFLAG': document['_id']['L_RETURNFLAG'],
            'L_LINESTATUS': document['_id']['L_LINESTATUS'],
            'SUM_QTY': document['SUM_QTY'],
            'SUM_BASE_PRICE': document['SUM_BASE_PRICE'],
            'SUM_DISC_PRICE': document['SUM_DISC_PRICE'],
            'SUM_CHARGE': document['SUM_CHARGE'],
            'AVG_QTY': document['AVG_QTY'],
            'AVG_PRICE': document['AVG_PRICE'],
            'AVG_DISC': document['AVG_DISC'],
            'COUNT_ORDER': document['COUNT_ORDER']
        }
        writer.writerow(row)
