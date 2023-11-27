import csv
import pymongo

client = pymongo.MongoClient("mongodb://mongodb:27017/")
database = client["tpch"]
lineitem = database["lineitem"]

pipeline = [
    {
        "$match": {
            "L_SHIPDATE": { "$gte": "1994-01-01", "$lt": "1995-01-01" },
            "L_DISCOUNT": { "$gte": .06 - 0.01, "$lte": .06 + 0.01 },
            "L_QUANTITY": { "$lt": 24 }
        }
    },
    {
        "$group": {
            "_id": None,
            "REVENUE": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", "$L_DISCOUNT"]}}
        }
    }
]

result = list(lineitem.aggregate(pipeline))

with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["REVENUE"])
    writer.writerow([result[0]["REVENUE"]])
