# query.py
from pymongo import MongoClient
import csv

# Database connection details
DB_NAME = "tpch"
DB_PORT = 27017
DB_HOST = "mongodb"

# Create MongoDB client
client = MongoClient(DB_HOST, DB_PORT)
db = client[DB_NAME]

# Query criteria for parts that are not of brand 45, not of size MEDIUM POLISHED and only parts of certain sizes
parts_query = {
    "P_BRAND": {"$ne": "Brand#45"},
    "P_TYPE": {"$not": {"$regex": "MEDIUM POLISHED.*"}},
    "P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}
}

# Find all distinct suppliers that supply qualified parts 
qualified_part_keys = db.part.distinct("P_PARTKEY", parts_query)
qualified_suppliers = db.partsupp.find({
    "PS_PARTKEY": {"$in": qualified_part_keys},
    "PS_SUPPKEY": {"$nin": db.supplier.find({"S_COMMENT": {"$regex": ".*Customer.*Complaints.*"}}).distinct("S_SUPPKEY")}
}, {"PS_SUPPKEY": 1}).distinct("PS_SUPPKEY")

# Count the number of parts supplied by each supplier according to the given criteria
supplier_counts = db.part.aggregate([
    {"$match": parts_query},
    {"$lookup": {
        "from": "partsupp",
        "localField": "P_PARTKEY",
        "foreignField": "PS_PARTKEY",
        "as": "partsupp"
    }},
    {"$unwind": "$partsupp"},
    {"$match": {
        "partsupp.PS_SUPPKEY": {"$in": qualified_suppliers}
    }},
    {"$group": {
        "_id": {
            "P_BRAND": "$P_BRAND",
            "P_TYPE": "$P_TYPE",
            "P_SIZE": "$P_SIZE"
        },
        "count": {"$sum": 1}
    }},
    {"$sort": {
        "count": -1,
        "_id.P_BRAND": 1,
        "_id.P_TYPE": 1,
        "_id.P_SIZE": 1
    }}
])

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['P_BRAND', 'P_TYPE', 'P_SIZE', 'SUPPLIER_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for doc in supplier_counts:
        row = {
            'P_BRAND': doc['_id']['P_BRAND'],
            'P_TYPE': doc['_id']['P_TYPE'],
            'P_SIZE': doc['_id']['P_SIZE'],
            'SUPPLIER_COUNT': doc['count']
        }
        writer.writerow(row)
