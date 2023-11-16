import pymongo
import csv

# Connect to the MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]
lineitem_collection = db["lineitem"]

# MongoDB query
query = {
    "L_SHIPDATE": {"$gte": "1994-01-01", "$lt": "1995-01-01"},
    "L_DISCOUNT": {"$gte": 0.05, "$lte": 0.07},
    "L_QUANTITY": {"$lt": 24}
}

# Project required fields for calculation
project = {
    "L_EXTENDEDPRICE": 1,
    "L_DISCOUNT": 1,
    "_id": 0
}

# Aggregate results according to the SQL query
pipeline = [
    {"$match": query},
    {"$project": project},
    {
        "$group": {
            "_id": None,
            "REVENUE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]}}
        }
    }
]

result = list(lineitem_collection.aggregate(pipeline))

# Write to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    if result:
        writer.writerow({'REVENUE': result[0]['REVENUE']})
    else:
        writer.writerow({'REVENUE': 0})

# Close the client
client.close()
