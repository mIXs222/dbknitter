from pymongo import MongoClient
import csv

# Connect to MongoDB
client = MongoClient(host='mongodb', port=27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# MongoDB aggregation pipeline
pipeline = [
    {"$match": {"L_SHIPDATE": {"$lte": "1998-09-02"}}},
    {"$group": {
        "_id": {"L_RETURNFLAG": "$L_RETURNFLAG", "L_LINESTATUS": "$L_LINESTATUS"},
        "SUM_QTY": {"$sum": "$L_QUANTITY"},
        "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
        "SUM_DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}] }},
        "SUM_CHARGE": {"$sum": 
                       {"$multiply": 
                        ["$L_EXTENDEDPRICE",
                         {"$add": 
                          [1, "$L_TAX"]}] }},
        "AVG_QTY": {"$avg": "$L_QUANTITY"},
        "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
        "AVG_DISC": {"$avg": "$L_DISCOUNT"},
        "COUNT_ORDER": {"$sum": 1}
    }},
    {"$sort": {"_id.L_RETURNFLAG": 1, "_id.L_LINESTATUS": 1}}
]

# Perform aggregation
results = list(lineitem_collection.aggregate(pipeline))

# Write results to CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    # Write header
    writer.writerow(['RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 
                     'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'])
    # Write data rows
    for row in results:
        writer.writerow(
            [row['_id']['L_RETURNFLAG'], row['_id']['L_LINESTATUS'], row['SUM_QTY'],
             row['SUM_BASE_PRICE'], row['SUM_DISC_PRICE'], row['SUM_CHARGE'],
             row['AVG_QTY'], row['AVG_PRICE'], row['AVG_DISC'], row['COUNT_ORDER']]
        )

# Close the MongoDB connection
client.close()
