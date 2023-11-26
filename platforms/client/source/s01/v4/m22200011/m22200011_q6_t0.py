from pymongo import MongoClient
import csv

# Connect to the MongoDB
client = MongoClient('mongodb', 27017)
db = client.tpch

# Define the pipeline for the aggregation
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {
                "$gte": "1994-01-01",
                "$lt": "1995-01-01"
            },
            "L_DISCOUNT": {
                "$gte": .05,
                "$lte": .07
            },
            "L_QUANTITY": {
                "$lt": 24
            }
        }
    },
    {
        "$group": {
            "_id": None,
            "REVENUE": {
                "$sum": {
                    "$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]
                }
            }
        }
    }
]

# Perform the aggregation
result = db.lineitem.aggregate(pipeline)

# Write the output to a CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(["REVENUE"])  # Headers

    for doc in result:
        writer.writerow([doc["REVENUE"]])
