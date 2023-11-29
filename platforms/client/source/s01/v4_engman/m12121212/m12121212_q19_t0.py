# queries.py
import pymongo
from direct_redis import DirectRedis
import pandas as pd

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_collection = mongo_db["part"]

# Redis connection setup
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Brands, containers, and sizes conditions for the three types of parts
conditions = [
    {"P_BRAND": "Brand#12", "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}, "P_SIZE": {"$gte": 1, "$lte": 5}},
    {"P_BRAND": "Brand#23", "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}, "P_SIZE": {"$gte": 1, "$lte": 10}},
    {"P_BRAND": "Brand#34", "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}, "P_SIZE": {"$gte": 1, "$lte": 15}}
]

part_keys = set()
for condition in conditions:
    for part_doc in mongo_collection.find(condition, {"_id": 0, "P_PARTKEY": 1}):
        part_keys.add(part_doc["P_PARTKEY"])

# Getting lineitem DataFrame
lineitem_df = pd.read_json(redis_client.get("lineitem"))

# Filtering data based on the given conditions
filtered_lineitem_df = lineitem_df[
    lineitem_df["L_PARTKEY"].isin(part_keys) &
    lineitem_df["L_SHIPMODE"].isin(["AIR", "AIR REG"]) &
    lineitem_df["L_SHIPINSTRUCT"].eq("DELIVER IN PERSON") &
    (
        (lineitem_df["L_QUANTITY"] >= 1) & (lineitem_df["L_QUANTITY"] <= 11) |
        (lineitem_df["L_QUANTITY"] >= 10) & (lineitem_df["L_QUANTITY"] <= 20) |
        (lineitem_df["L_QUANTITY"] >= 20) & (lineitem_df["L_QUANTITY"] <= 30)
    )
]

# Calculating revenue
filtered_lineitem_df["REVENUE"] = filtered_lineitem_df["L_EXTENDEDPRICE"] * (1 - filtered_lineitem_df["L_DISCOUNT"])
revenue = filtered_lineitem_df["REVENUE"].sum()

# Writing result to CSV file
results_df = pd.DataFrame({"REVENUE": [revenue]})
results_df.to_csv("query_output.csv", index=False)
