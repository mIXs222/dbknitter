from pymongo import MongoClient
import csv

client = MongoClient('mongodb://localhost:27017/')
db = client['tpch']
collection = db['lineitem']

pipeline = [
    {"$match": { 
        "L_SHIPDATE": {"$gte": '1994-01-01', "$lt": '1995-01-01'},
        "L_DISCOUNT": {"$gte": .06 - 0.01, "$lte": .06 + 0.01},
        "L_QUANTITY": {"$lt": 24} 
    }}, 
    {"$project": {
        "REVENUE": {"$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]}
    }}, 
    {"$group": {
        "_id": None, 
        "REVENUE": {"$sum": "$REVENUE"}
    }}
]

result = collection.aggregate(pipeline)
with open('query_output.csv', 'w', newline='') as file:
    fieldnames = ['REVENUE']
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    writer.writeheader()
    for res in result:
        writer.writerow({"REVENUE": res["REVENUE"]})
