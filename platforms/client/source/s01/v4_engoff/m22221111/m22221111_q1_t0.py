# Python code to execute the query on MongoDB and write output to a CSV file.

from pymongo import MongoClient
import csv
from datetime import datetime

# Connect to the MongoDB server
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Define the query to extract data from the 'lineitem' collection
query = {
    'L_SHIPDATE': {"$lt": datetime(1998, 9, 2)}
}

# Define the aggregation pipeline to perform the summarized calculations
pipeline = [
    {"$match": query},
    {"$group": {
        "_id": {"L_RETURNFLAG": "$L_RETURNFLAG", "L_LINESTATUS": "$L_LINESTATUS"},
        "SUM_QTY": {"$sum": "$L_QUANTITY"},
        "SUM_BASE_PRICE": {"$sum": "$L_EXTENDEDPRICE"},
        "SUM_DISC_PRICE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}] }},
        "SUM_CHARGE": {"$sum": {
            "$multiply": [
                {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]},
                {"$add": [1, "$L_TAX"]}
            ]}},
        "AVG_QTY": {"$avg": "$L_QUANTITY"},
        "AVG_PRICE": {"$avg": "$L_EXTENDEDPRICE"},
        "AVG_DISC": {"$avg": "$L_DISCOUNT"},
        "COUNT_ORDER": {"$sum": 1}
    }},
    {"$sort": {"_id.L_RETURNFLAG": 1, "_id.L_LINESTATUS": 1}}
]

# Perform the aggregation query
result = db['lineitem'].aggregate(pipeline)

# Write the query result to a CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    # Write the header row
    writer.writerow(["L_RETURNFLAG", "L_LINESTATUS", "SUM_QTY", "SUM_BASE_PRICE", "SUM_DISC_PRICE", "SUM_CHARGE", "AVG_QTY", "AVG_PRICE", "AVG_DISC", "COUNT_ORDER"])

    for doc in result:
        writer.writerow([
            doc["_id"]["L_RETURNFLAG"],
            doc["_id"]["L_LINESTATUS"],
            doc["SUM_QTY"],
            doc["SUM_BASE_PRICE"],
            doc["SUM_DISC_PRICE"],
            doc["SUM_CHARGE"],
            doc["AVG_QTY"],
            doc["AVG_PRICE"],
            doc["AVG_DISC"],
            doc["COUNT_ORDER"]
        ])

print("Query results have been written to query_output.csv")

# Close the MongoDB client connection
client.close()
