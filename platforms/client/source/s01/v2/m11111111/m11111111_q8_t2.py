import pandas as pd
from pymongo import MongoClient
from pandas.io.json import json_normalize

client = MongoClient('mongodb://localhost:27017')
db = client['tpch']

# Translate the SQL query into mongodb's specific operators.
pipeline = [
    {"$match": {
        "P_TYPE": "SMALL PLATED COPPER",
        "O_ORDERDATE": {"$gte": "1995-01-01", "$lte": "1996-12-31"}
    }},
    {"$unwind": "$orders"},
    {"$lookup": {
        "from": "customer",
        "localField": "O_CUSTKEY",
        "foreignField": "C_CUSTKEY",
        "as": "customerInfo"
    }},
    {"$unwind": "$customerInfo"},
    # More $lookup and $unwind steps here for other tables...
    {"$group": {
        "_id": { "year": { "$year": "$O_ORDERDATE" }},
        "totalVolume": { "$sum": { "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}] }},
        "indianVolume": { "$sum": {"$cond": [{ "$eq": ["$NATION", "INDIA"] }, { "$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}] }, 0] }}
    }},
    {"$project": {
        "year": "$_id.year",
        "mktShare": { "$divide": ["$indianVolume", "$totalVolume"] }
    }},
    {"$sort": { "year": -1 }}
]    

# Execute the pipeline.
result = db.part.aggregate(pipeline)

df = pd.json_normalize(list(result))
df.to_csv('query_output.csv', index=False)
