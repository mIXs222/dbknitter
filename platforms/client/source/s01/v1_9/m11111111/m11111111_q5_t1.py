from pymongo import MongoClient
import pandas as pd
from bson.son import SON


client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

pipeline = [
    {"$lookup": {
         "from": "lineitem",
         "localField": "O_ORDERKEY",
         "foreignField": "L_ORDERKEY",
         "as": "lineitem_joined"}},
    {"$unwind": "$lineitem_joined"},
    {"$lookup": {
         "from": "supplier",
         "localField": "lineitem_joined.L_SUPPKEY",
         "foreignField": "S_SUPPKEY",
         "as": "supplier_joined"}},
    {"$unwind": "$supplier_joined"},
    {"$lookup": {
         "from": "nation",
         "localField": "supplier_joined.S_NATIONKEY",
         "foreignField": "N_NATIONKEY",
         "as": "nation_joined"}},
    {"$unwind": "$nation_joined"},
    {"$lookup": {
         "from": "region",
         "localField": "nation_joined.N_REGIONKEY",
         "foreignField": "R_REGIONKEY",
         "as": "region_joined"}},
    {"$unwind": "$region_joined"},
    {"$match": {
         "region_joined.R_NAME": "ASIA",
         "O_ORDERDATE": {"$gte": pd.to_datetime("1990-01-01"), "$lt": pd.to_datetime("1995-01-01")}}},
    {"$group": {
         "_id": "$nation_joined.N_NAME",
         "REVENUE": {"$sum": {"$multiply": ["$lineitem_joined.L_EXTENDEDPRICE", {"$subtract": [1, "$lineitem_joined.L_DISCOUNT"]}]}}}},
    {"$sort": SON([("REVENUE", -1)])}
]

result = db.orders.aggregate(pipeline)

out_df = pd.DataFrame(list(result))
out_df.columns = ['N_NAME', 'REVENUE']
out_df.to_csv('query_output.csv', index=False)
