from pymongo import MongoClient
import csv

# Connect to the MongoDB instance
client = MongoClient('mongodb', 27017)
db = client['tpch']

# MongoDB query to find lineitems matching the conditions
pipeline = [
    {
        "$match": {
            "L_SHIPDATE": {"$gt": "1994-01-01", "$lt": "1995-01-01"},
            "L_DISCOUNT": {"$gte": 0.01, "$lte": 0.07},
            "L_QUANTITY": {"$lt": 24}
        }
    },
    {
        "$project": {
            "_id": 0,
            "REVENUE": {"$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]}
        }
    },
    {
        "$group": {
            "_id": None,
            "TOTAL_REVENUE": {"$sum": "$REVENUE"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "TOTAL_REVENUE": 1
        }
    }
]

# Execute the MongoDB aggregation pipeline
result = list(db.lineitem.aggregate(pipeline))

# Write the result to query_output.csv
with open('query_output.csv', 'w', newline='') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(['REVENUE'])  # Writing header
    for doc in result:
        writer.writerow([doc['TOTAL_REVENUE']])

# Close the MongoDB connection
client.close()
