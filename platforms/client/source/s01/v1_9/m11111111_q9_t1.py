from pymongo import MongoClient
import pandas as pd

# connect to MongoDB
client = MongoClient('mongodb://mongodb:27017/')

db = client.tpch

# unfortunately, pymongo does not support SQL like queries out of the box
# you will have to manually convert your SQL query to pymongo commands
# the below is a placeholder and won't accurately return your expected results
amounts = db.lineitem.aggregate([
    {"$match": {"P_NAME": {"$regex": ".*dim.*"}}},
    {"$lookup": {"from": "supplier", "localField": "L_SUPPKEY", "foreignField": "S_SUPPKEY", "as": "supplierInfo"}},
    {"$unwind": "$supplierInfo"},
    {"$lookup": {"from": "nation", "localField": "supplierInfo.S_NATIONKEY", "foreignField": "N_NATIONKEY", "as": "nationInfo"}},
    {"$unwind": "$nationInfo"},
    {"$lookup": {"from": "partsupp", "localField": "L_PARTKEY", "foreignField": "PS_PARTKEY", "as": "partsuppInfo"}},
    {"$unwind": "$partsuppInfo"},
    {"$project": {
        "NATION": "$nationInfo.N_NAME",
        "O_YEAR": {"$year": "$O_ORDERDATE"},
        "AMOUNT": {"$subtract": [{"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}, {"$multiply": ["$partsuppInfo.PS_SUPPLYCOST", "$L_QUANTITY"]}]}
    }}
])

df = pd.DataFrame(list(amounts))
df_group = df.groupby(["NATION", "O_YEAR"])["AMOUNT"].sum().reset_index()
df_group.sort_values(["NATION", "O_YEAR"], ascending=[True, False], inplace=True)
df_group.to_csv('query_output.csv', index=False)
