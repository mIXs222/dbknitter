import pymongo
import csv

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
db = client["tpch"]

# Perform the query
pipeline = [
    {
        "$lookup": {
            "from": "supplier",
            "localField": "PS_SUPPKEY",
            "foreignField": "S_SUPPKEY",
            "as": "supplier"
        }
    },
    { "$unwind": "$supplier" },
    {
        "$lookup": {
            "from": "nation",
            "localField": "supplier.S_NATIONKEY",
            "foreignField": "N_NATIONKEY",
            "as": "nation"
        }
    },
    { "$unwind": "$nation" },
    { "$match": { "nation.N_NAME": "GERMANY" } },
    {
        "$project": {
            "PS_PARTKEY": 1,
            "TotalValue": { "$multiply": ["$PS_SUPPLYCOST", "$PS_AVAILQTY"] }
        }
    },
    { "$group": {
        "_id": "$PS_PARTKEY",
        "TotalValueSum": { "$sum": "$TotalValue" }
    }},
    # Add a threshold calculation if needed. This example uses a fixed threshold value (e.g., 1000).
    # This should be replaced with the calculation logic required for the specific analysis.
    { "$match": { "TotalValueSum": { "$gt": 1000 } } },
    { "$sort": { "TotalValueSum": -1 } }
]

result = db["partsupp"].aggregate(pipeline)

# Write output to a CSV file
with open('query_output.csv', mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(["PS_PARTKEY", "TotalValueSum"])

    for data in result:
        writer.writerow([data["_id"], data["TotalValueSum"]])
