import csv
import pymongo
from pymongo import MongoClient

connection = MongoClient("mongodb", 27017)
db = connection.tpch

result = db.lineitem.aggregate([
    {"$lookup": {
        "from": "part",
        "localField": "L_PARTKEY",
        "foreignField": "P_PARTKEY",
        "as": "part_docs"
    }},
    {"$unwind": "$part_docs"},
    {"$match": {"part_docs.P_NAME": {"$regex": ".*dim.*"}}},
    {"$lookup": {
        "from": "supplier",
        "localField": "L_SUPPKEY",
        "foreignField": "S_SUPPKEY",
        "as": "supplier_docs"
    }},
    {"$unwind": "$supplier_docs"},
    {"$lookup": {
        "from": "partsupp",
        "let": {"partkey": "$L_PARTKEY", "suppkey": "$L_SUPPKEY"},
        "pipeline": [
            {"$match":
                {"$expr":
                    {"$and": [
                        {"$eq": ["$PS_PARTKEY", "$$partkey"]},
                        {"$eq": ["$PS_SUPPKEY", "$$suppkey"]},
                    ]}
                }
            }
        ],
        "as": "partsupp_docs"
    }},
    {"$unwind": "$partsupp_docs"},
    ... # continue with the similar lookup for orders and nation tables
], allowDiskUse=True)

with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["NATION", "O_YEAR", "SUM_PROFIT"])
    for r in result:
        writer.writerow([r["nation"], r["O_YEAR"], r["SUM_PROFIT"]]) 
