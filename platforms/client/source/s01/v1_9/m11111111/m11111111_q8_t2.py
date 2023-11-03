from pymongo import MongoClient
import pandas as pd
from bson.son import SON

# Connecting to MongoDB
client = MongoClient("mongodb://mongodb:27017/")
db = client.tpch

# Aggregation pipeline to run the SQL equivalent MongoDB query
pipeline = [
    {"$lookup": {"from": "nation1", "localField": "C_NATIONKEY", "foreignField": "N_NATIONKEY", "as": "nation1"}},
    {"$unwind": "$nation1"},
    {"$lookup": {"from": "orders", "localField": "O_CUSTKEY", "foreignField": "O_ORDERKEY", "as": "orders"}},
    {"$unwind": "$orders"},
    {"$lookup": {"from": "lineitem", "localField": "O_ORDERKEY", "foreignField": "L_ORDERKEY", "as": "lineitem"}},
    {"$unwind": "$lineitem"},
    {"$lookup": {"from": "supplier", "localField": "L_SUPPKEY", "foreignField": "S_SUPPKEY", "as": "supplier"}},
    {"$unwind": "$supplier"},
    {"$lookup": {"from": "part", "localField": "L_PARTKEY", "foreignField": "P_PARTKEY", "as": "part"}},
    {"$unwind": "$part"},
    {"$lookup": {"from": "nation2", "localField": "S_NATIONKEY", "foreignField": "N_NATIONKEY", "as": "nation2"}},
    {"$unwind": "$nation2"},
    {"$lookup": {"from": "region", "localField": "N_REGIONKEY", "foreignField": "R_REGIONKEY", "as": "region"}},
    {"$unwind": "$region"},
    {"$match": {
        "O_ORDERDATE": {"$gte": "1995-01-01", "$lte": "1996-12-31"},
        "region.R_NAME": "ASIA",
        "part.P_TYPE": "SMALL PLATED COPPER"}},
    {"$group": {
        "_id": {"year": {"$year": "$O_ORDERDATE"}},
        "volume": {"$sum": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}}}},
    {"$project": {
        "O_YEAR": "$_id.year",
        "MKT_SHARE": {
            "$cond": [{"$eq": ["$NATION", "INDIA"]}, {"$divide": ["$volume", "$volume"]}, 0]}}},
    {"$sort": SON([("O_YEAR", -1)])}
]

# Execute the pipeline on the 'customer' collection
results = db.customer.aggregate(pipeline)

# Convert result to dataframe
df = pd.DataFrame(list(results))

# Write the dataframe to csv file
df.to_csv('query_output.csv', index=False)
