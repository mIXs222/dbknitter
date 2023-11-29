from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch
lineitem_collection = db.lineitem

# The query
pipeline = [
    {"$match": {
        "L_SHIPDATE": {"$lt": datetime(1998, 9, 2)}
    }},
    {"$group": {
        "_id": {"RETURNFLAG": "$L_RETURNFLAG", "LINESTATUS": "$L_LINESTATUS"},
        "SUM_QTY": {"$sum": "$L_QUANTITY"},
        "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
        "SUM_DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}] }},
        "SUM_CHARGE": {"$sum": {
            "$multiply": [ 
                {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}] },
                {"$add": [1, "$L_TAX"]}]
            }
        },
        "AVG_QTY": {"$avg": "$L_QUANTITY"},
        "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
        "AVG_DISC": {"$avg": "$L_DISCOUNT"},
        "COUNT_ORDER": {"$sum": 1}
    }},
    {"$sort": {"_id.RETURNFLAG": 1, "_id.LINESTATUS": 1}}
]

# Perform the query
result = lineitem_collection.aggregate(pipeline)

# Writing to CSV
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    # Write the headers
    writer.writerow(['RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 
                     'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 
                     'AVG_DISC', 'COUNT_ORDER'])
    
    # Write the data rows
    for data in result:
        writer.writerow([
            data['_id']['RETURNFLAG'],
            data['_id']['LINESTATUS'],
            data['SUM_QTY'],
            data['SUM_BASE_PRICE'],
            data['SUM_DISC_PRICE'],
            data['SUM_CHARGE'],
            data['AVG_QTY'],
            data['AVG_PRICE'],
            data['AVG_DISC'],
            data['COUNT_ORDER']
        ])

# Close the connection
client.close()
