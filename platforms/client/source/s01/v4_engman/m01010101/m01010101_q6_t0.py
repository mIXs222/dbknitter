import pymongo
import csv
import os

# Establish a connection to the MongoDB server
client = pymongo.MongoClient("mongodb", 27017)

# Select the relevant database and collection
db = client['tpch']
lineitem = db['lineitem']

# Query lineitems with the specified conditions
pipeline = [
    {
        "$match": {
            "$expr": {
                "$and": [
                    {"$gt": ["$L_SHIPDATE", "1994-01-01"]},
                    {"$lt": ["$L_SHIPDATE", "1995-01-01"]},
                    {"$gte": ["$L_DISCOUNT", 0.06 - 0.01]},
                    {"$lte": ["$L_DISCOUNT", 0.06 + 0.01]},
                    {"$lt": ["$L_QUANTITY", 24]}
                ]
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
    },
    {
        "$project": {
            "_id": 0,
            "REVENUE": 1
        }
    }
]

# Execute aggregation
result = list(lineitem.aggregate(pipeline))

# Write the output to a CSV file
output_file = 'query_output.csv'
os.makedirs(os.path.dirname(output_file), exist_ok=True)
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write headers
    writer.writerow(['REVENUE'])
    # Write data
    for data in result:
        writer.writerow([data['REVENUE']])

# Close the connection to the MongoDB server
client.close()
