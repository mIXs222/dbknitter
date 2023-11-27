import pymongo
import csv
from datetime import datetime

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

# Specify the dim to search for in the part names
specified_dim = "SPECIFIED_DIM"  # Replace SPECIFIED_DIM with the actual dim to be searched

# Query MongoDB
pipeline = [
    {
        "$lookup": {
            "from": "supplier",
            "localField": "L_SUPPKEY",
            "foreignField": "S_SUPPKEY",
            "as": "supplier_info"
        }
    },
    {"$unwind": "$supplier_info"},
    {
        "$lookup": {
            "from": "partsupp",
            "let": {"part_key": "$L_PARTKEY", "supp_key": "$L_SUPPKEY"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [{"$eq": ["$PS_PARTKEY", "$$part_key"]},
                                                {"$eq": ["$PS_SUPPKEY", "$$supp_key"]}]}}},
            ],
            "as": "partsupp_info"
        }
    },
    {"$unwind": "$partsupp_info"},
    {
        "$lookup": {
            "from": "part",
            "localField": "L_PARTKEY",
            "foreignField": "P_PARTKEY",
            "as": "part_info"
        }
    },
    {"$unwind": "$part_info"},
    {
        "$match": {
            "part_info.P_NAME": {"$regex": specified_dim}
        }
    },
    {
        "$project": {
            "profit": {
                "$subtract": [
                    {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]},
                    {"$multiply": ["$partsupp_info.PS_SUPPLYCOST", "$L_QUANTITY"]}
                ]
            },
            "nation": "$supplier_info.S_NATIONKEY",
            "year": {"$year": "$L_SHIPDATE"}
        }
    },
    {
        "$group": {
            "_id": {
                "nation": "$nation",
                "year": "$year"
            },
            "profit": {"$sum": "$profit"}
        }
    },
    {"$sort": {"_id.nation": 1, "_id.year": -1}},
    {
        "$project": {
            "_id": 0,
            "nation": "$_id.nation",
            "year": "$_id.year",
            "profit": "$profit"
        }
    }
]

result = list(db['lineitem'].aggregate(pipeline))

# Adding nation names to the result
nation_info = {n['N_NATIONKEY']: n['N_NAME'] for n in db['nation'].find()}
for r in result:
    r['nation'] = nation_info.get(r['nation'], 'Unknown')

# Write result to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['nation', 'year', 'profit']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for data in result:
        writer.writerow(data)

print("Query results have been saved to query_output.csv.")
