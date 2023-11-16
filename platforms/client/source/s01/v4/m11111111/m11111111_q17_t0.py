from pymongo import MongoClient
import csv

# MongoDB connection information
MONGO_DB = "tpch"
MONGO_PORT = 27017 
MONGO_HOSTNAME = "mongodb"

def connect_to_mongo():
    client = MongoClient(MONGO_HOSTNAME, MONGO_PORT)
    db = client[MONGO_DB]
    return db

# Connect to MongoDB
db = connect_to_mongo()

parts = list(db.part.find({"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"}, {"P_PARTKEY": 1}))
part_keys = [p["P_PARTKEY"] for p in parts]

# Initialize variable to hold sum of extended prices
total_extended_price = 0

# Process lineitems that have a partkey in part_keys
for part_key in part_keys:
    avg_quantity = db.lineitem.aggregate([
        {"$match": {"L_PARTKEY": part_key}},
        {"$group": {"_id": None, "avg_quantity": {"$avg": "$L_QUANTITY"}}}
    ]).next()["avg_quantity"]

    lineitems = db.lineitem.find(
        {
            "L_PARTKEY": part_key,
            "L_QUANTITY": {"$lt": 0.2 * avg_quantity}
        },
        {"L_EXTENDEDPRICE": 1}
    )
    
    for lineitem in lineitems:
        total_extended_price += lineitem["L_EXTENDEDPRICE"]

avg_yearly = total_extended_price / 7.0

# Write the output to query_output.csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['AVG_YEARLY'])
    writer.writerow([avg_yearly])

print("Query output has been written to query_output.csv.")
