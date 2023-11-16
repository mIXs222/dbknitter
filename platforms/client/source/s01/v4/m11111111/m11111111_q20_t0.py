# query.py
from pymongo import MongoClient
from datetime import datetime
import csv

# Given constants
MONGO_DB_NAME = 'tpch'
MONGO_PORT = 27017
MONGO_HOSTNAME = 'mongodb'

# Connect to MongoDB
client = MongoClient(MONGO_HOSTNAME, MONGO_PORT)
db = client[MONGO_DB_NAME]

# Find part keys with names starting with 'forest'
parts = db.part.find({"P_NAME": {"$regex": '^forest'}}, {"P_PARTKEY": 1})
part_keys = [p['P_PARTKEY'] for p in parts]

# Find suppliers with parts and availability
partsupps = db.partsupp.aggregate([
    {
        "$match": {
            "PS_PARTKEY": {"$in": part_keys}
        }
    },
    {
        "$lookup": {
            "from": "lineitem",
            "let": {"ps_partkey": "$PS_PARTKEY", "ps_suppkey": "$PS_SUPPKEY"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$L_PARTKEY", "$$ps_partkey"]},
                            {"$eq": ["$L_SUPPKEY", "$$ps_suppkey"]},
                            {"$gte": ["$L_SHIPDATE", datetime(1994, 1, 1)]},
                            {"$lt": ["$L_SHIPDATE", datetime(1995, 1, 1)]}
                        ]
                    }
                }},
                {"$group": {
                    "_id": None,
                    "total_quantity": {"$sum": "$L_QUANTITY"}
                }}
            ],
            "as": "lineitems"
        }
    },
    {"$match": {"$expr": {"$gt": ["$PS_AVAILQTY", {"$multiply": [0.5, {"$ifNull": [{"$arrayElemAt": ["$lineitems.total_quantity", 0]}, 0]}]}]}}},
    {"$project": {"_id": 0, "PS_SUPPKEY": 1}}
])

supplier_keys = [ps['PS_SUPPKEY'] for ps in partsupps]

# Find suppliers in Canada
suppliers = db.supplier.aggregate([
    {
        "$match": {
            "S_SUPPKEY": {"$in": supplier_keys}
        }
    },
    {
        "$lookup": {
            "from": "nation",
            "localField": "S_NATIONKEY",
            "foreignField": "N_NATIONKEY",
            "as": "nation_info"
        }
    },
    {"$unwind": "$nation_info"},
    {"$match": {"nation_info.N_NAME": "CANADA"}},
    {"$project": {"_id": 0, "S_NAME": 1, "S_ADDRESS": 1}}
])

# Output the query result to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['S_NAME', 'S_ADDRESS'])  # header

    for supplier in suppliers:
        writer.writerow([supplier['S_NAME'], supplier['S_ADDRESS']])
