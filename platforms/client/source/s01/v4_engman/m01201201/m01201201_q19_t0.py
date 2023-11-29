import pymongo
import pandas as pd

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# MongoDB query
query = {
    "L_SHIPMODE": {"$in": ["AIR", "AIR REG"]},
    "L_SHIPINSTRUCT": "DELIVER IN PERSON",
    "$or": [
        {
            "L_QUANTITY": {"$gte": 1, "$lte": 11},
            "L_PARTKEY": "/^12/",
            "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}
        },
        {
            "L_QUANTITY": {"$gte": 10, "$lte": 20},
            "L_PARTKEY": "/^23/",
            "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}
        },
        {
            "L_QUANTITY": {"$gte": 20, "$lte": 30},
            "L_PARTKEY": "/^34/",
            "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}
        }
    ]
}

lineitem_df = pd.DataFrame(list(lineitem_collection.find(query, {"L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1})))
lineitem_df["REVENUE"] = lineitem_df["L_EXTENDEDPRICE"] * (1 - lineitem_df["L_DISCOUNT"])

# Aggregate to calculate the revenue
total_revenue = lineitem_df["REVENUE"].sum()

# Write to CSV
pd.DataFrame({'REVENUE': [total_revenue]}).to_csv("query_output.csv", index=False)
