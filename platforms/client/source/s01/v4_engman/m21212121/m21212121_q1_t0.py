from pymongo import MongoClient
import csv
import os

# Connect to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem = db['lineitem']

# Define pipeline for the aggregation
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {"$lt": "1998-09-02"}
        }
    },
    {
        "$group": {
            "_id": {
                "RETURNFLAG": "$L_RETURNFLAG",
                "LINESTATUS": "$L_LINESTATUS"
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
        "$sort": {"_id.RETURNFLAG": 1, "_id.LINESTATUS": 1}
    }
]

# Execute aggregation
result = list(lineitem.aggregate(pipeline))

# Write the results to a CSV file
output_file = 'query_output.csv'
fields = ['RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER']

# Create a directory if it doesn't exist
os.makedirs(os.path.dirname(output_file), exist_ok=True)

with open(output_file, mode='w') as csvfile:
    csvwriter = csv.DictWriter(csvfile, fieldnames=fields)
    csvwriter.writeheader()
    for data in result:
        csvwriter.writerow({
            "RETURNFLAG": data['_id']['RETURNFLAG'],
            "LINESTATUS": data['_id']['LINESTATUS'],
            "SUM_QTY": data['SUM_QTY'],
            "SUM_BASE_PRICE": data['SUM_BASE_PRICE'],
            "SUM_DISC_PRICE": data['SUM_DISC_PRICE'],
            "SUM_CHARGE": data['SUM_CHARGE'],
            "AVG_QTY": data['AVG_QTY'],
            "AVG_PRICE": data['AVG_PRICE'],
            "AVG_DISC": data['AVG_DISC'],
            "COUNT_ORDER": data['COUNT_ORDER'],
        })

# Close client connection
client.close()
