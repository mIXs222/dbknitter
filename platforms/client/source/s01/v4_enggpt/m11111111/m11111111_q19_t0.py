import pymongo
import csv

# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]
part_collection = db["part"]
lineitem_collection = db["lineitem"]

# Define the brand, container, etc. criteria
conditions = [
    {
        "P_BRAND": "Brand#12",
        "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]},
        "L_QUANTITY": {"$gte": 1, "$lte": 11},
        "P_SIZE": {"$gte": 1, "$lte": 5},
        "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]},
        "L_SHIPINSTRUCT": "DELIVER IN PERSON"
    },
    {
        "P_BRAND": "Brand#23",
        "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]},
        "L_QUANTITY": {"$gte": 10, "$lte": 20},
        "P_SIZE": {"$gte": 1, "$lte": 10},
        "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]},
        "L_SHIPINSTRUCT": "DELIVER IN PERSON"
    },
    {
        "P_BRAND": "Brand#34",
        "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]},
        "L_QUANTITY": {"$gte": 20, "$lte": 30},
        "P_SIZE": {"$gte": 1, "$lte": 15},
        "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]},
        "L_SHIPINSTRUCT": "DELIVER IN PERSON"
    }
]

# Query the MongoDB
match_conditions = [{"$or": conditions}]
pipeline = [
    {"$lookup": {
        "from": "lineitem",
        "localField": "P_PARTKEY",
        "foreignField": "L_PARTKEY",
        "as": "lineitems"
    }},
    {"$unwind": "$lineitems"},
    {"$match": {"$or": conditions}},
    {"$project": {
        "revenue": {
            "$multiply": [
                "$lineitems.L_EXTENDEDPRICE",
                {"$subtract": [1, "$lineitems.L_DISCOUNT"]}
            ]
        }
    }},
    {"$group": {
        "_id": None,
        "total_revenue": {"$sum": "$revenue"}
    }}
]

results = list(part_collection.aggregate(pipeline))

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['total_revenue']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in results:
        writer.writerow({'total_revenue': data['total_revenue']})
