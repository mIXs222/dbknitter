from pymongo import MongoClient
import csv
from datetime import datetime

# MongoDB connection setup
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem = db['lineitem']

# Perform the query
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {"$lt": datetime(1998, 9, 2)}
        }
    },
    {
        "$group": {
            "_id": {"RETURNFLAG": "$L_RETURNFLAG", "LINESTATUS": "$L_LINESTATUS"},
            "QUANTITY": {"$sum": "$L_QUANTITY"},
            "EXTENDEDPRICE": {"$sum": "$L_EXTENDEDPRICE"},
            "DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}},
            "CHARGE": {"$sum": {"$multiply": [{"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}, {"$add": [1, "$L_TAX"]}]}},
            "AVERAGE_QUANTITY": {"$avg": "$L_QUANTITY"},
            "AVERAGE_EXTENDEDPRICE": {"$avg": "$L_EXTENDEDPRICE"},
            "AVERAGE_DISCOUNT": {"$avg": "$L_DISCOUNT"},
            "COUNT_ORDER": {"$sum": 1}
        }
    },
    {
        "$sort": {
            "_id.RETURNFLAG": 1,
            "_id.LINESTATUS": 1
        }
    }
]

results = lineitem.aggregate(pipeline)

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['RETURNFLAG', 'LINESTATUS', 'QUANTITY', 'EXTENDEDPRICE', 'DISC_PRICE', 'CHARGE', 'AVERAGE_QUANTITY', 'AVERAGE_EXTENDEDPRICE', 'AVERAGE_DISCOUNT', 'COUNT_ORDER']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for result in results:
        writer.writerow({
            'RETURNFLAG': result['_id']['RETURNFLAG'],
            'LINESTATUS': result['_id']['LINESTATUS'],
            'QUANTITY': result['QUANTITY'],
            'EXTENDEDPRICE': result['EXTENDEDPRICE'],
            'DISC_PRICE': result['DISC_PRICE'],
            'CHARGE': result['CHARGE'],
            'AVERAGE_QUANTITY': result['AVERAGE_QUANTITY'],
            'AVERAGE_EXTENDEDPRICE': result['AVERAGE_EXTENDEDPRICE'],
            'AVERAGE_DISCOUNT': result['AVERAGE_DISCOUNT'],
            'COUNT_ORDER': result['COUNT_ORDER']
        })

client.close()
