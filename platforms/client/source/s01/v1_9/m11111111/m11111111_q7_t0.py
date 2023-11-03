from pymongo import MongoClient
import pandas as pd
from datetime import datetime

client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

pipeline = [
    {"$match": {
        "$or": [
            {"N1.N_NAME": "JAPAN", "N2.N_NAME": "INDIA"},
            {"N1.N_NAME": "INDIA", "N2.N_NAME": "JAPAN"}
        ],
        "L_SHIPDATE": {"$gte": datetime(1995, 1, 1), "$lte": datetime(1996, 12, 31)}
    }},
    {"$lookup": {"from": "supplier", "localField": "S_SUPPKEY", "foreignField": "S_SUPPKEY", "as": "supplier_info"}},
    {"$lookup": {"from": "lineitem", "localField": "L_ORDERKEY", "foreignField": "O_ORDERKEY", "as": "lineitem_info"}},
    {"$lookup": {"from": "orders", "localField": "O_ORDERKEY", "foreignField": "C_CUSTKEY", "as": "orders_info"}},
    {"$lookup": {"from": "customer", "localField": "C_CUSTKEY", "foreignField": "O_CUSTKEY", "as": "customer_info"}},
    {"$lookup": {"from": "nation", "localField": "S_NATIONKEY", "foreignField": "N1.N_NATIONKEY", "as": "nation1_info"}},
    {"$lookup": {"from": "nation", "localField": "C_NATIONKEY", "foreignField": "N2.N_NATIONKEY", "as": "nation2_info"}},
    {"$project": {
        "SUPP_NATION": "$nation1_info.N_NAME",
        "CUST_NATION": "$nation2_info.N_NAME",
        "L_YEAR": {"$year": "$L_SHIPDATE"},
        "VOLUME": {"$multiply": ["$L_EXTENDEDPRICE", {"$subtract": [1, "$L_DISCOUNT"]}]}
    }},
    {"$group": {
        "_id": {"SUPP_NATION": "$SUPP_NATION", "CUST_NATION": "$CUST_NATION", "L_YEAR": "$L_YEAR"},
        "REVENUE": {"$sum": "$VOLUME"}
    }},
    {"$sort": {"_id": 1}}
]

data_cursor = db.lineitem.aggregate(pipeline)
data_list = list(data_cursor)
df = pd.DataFrame(data_list)
df.to_csv('query_output.csv', index=False)
