from pymongo import MongoClient
from bson.json_util import dumps
import json
import pandas as pd
from datetime import datetime


client = MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]

def get_data():
    
    pipeline = [
        {"$match": {"R_NAME": "ASIA"}},
        {"$lookup": {"from": "nation", "localField": "R_REGIONKEY", "foreignField": "N_REGIONKEY", "as": "nation"}},
        {"$unwind": "$nation"},
        {"$lookup": {"from": "supplier", "localField": "nation.N_NATIONKEY", "foreignField": "S_NATIONKEY", "as": "supplier"}},
        {"$unwind": "$supplier"},
        {"$lookup": {"from": "partsupp", "localField": "supplier.S_SUPPKEY", "foreignField": "PS_SUPPKEY", "as": "partsupp"}},
        {"$unwind": "$partsupp"},  
        {"$lookup": {"from": "lineitem", "localField": "partsupp.PS_PARTKEY", "foreignField": "L_PARTKEY", "as": "lineitem"}},
        {"$unwind": "$lineitem"},
        {"$lookup": {"from": "orders", "localField": "lineitem.L_ORDERKEY", "foreignField": "O_ORDERKEY", "as": "orders"}},
        {"$unwind": "$orders"},
        {"$lookup": {"from": "customer", "localField": "orders.O_CUSTKEY", "foreignField": "C_CUSTKEY", "as": "customer"}},
        {"$unwind": "$customer"},
        {"$match": { "orders.O_ORDERDATE": {"$gte": datetime.strptime('1990-01-01', "%Y-%m-%d"), "$lt": datetime.strptime('1995-01-01', "%Y-%m-%d")}}},
        {"$group": {"_id": "$nation.N_NAME", "REVENUE": {"$sum": {"$multiply": ['$lineitem.L_EXTENDEDPRICE', {'$subtract': [1, '$lineitem.L_DISCOUNT']}]} }}},
        {"$sort": {"REVENUE": -1}}
    ]
    
    data = list(db.region.aggregate(pipeline))
    return data


def write_to_csv(data):
    df = pd.DataFrame(data)
    df["_id"].to_csv("query_output.csv", index=False)


def main():
    data = get_data()
    write_to_csv(data)


if __name__ == "__main__":
    main()
