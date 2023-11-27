# MinimumCostSupplierQuery.py
from pymongo import MongoClient
import direct_redis
import pandas as pd

# Initialize MongoDB connection
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Load supplier and partsupp tables
suppliers = pd.DataFrame(list(mongo_db["supplier"].find()))
partsupp = pd.DataFrame(list(mongo_db["partsupp"].find()))

# Initialize Redis connection
redis_client = direct_redis.DirectRedis(host="redis", port=6379)

# Load nation, region, and part tables from Redis
nation = pd.read_json(redis_client.get("nation"))
region = pd.read_json(redis_client.get("region"))
part = pd.read_json(redis_client.get("part"))

# Filter part by BRASS type and size 15
part = part[(part["P_TYPE"] == "BRASS") & (part["P_SIZE"] == 15)]

# Filter region to keep only EUROPE
region = region[region["R_NAME"] == "EUROPE"]

# Join tables to gather required information
result = (
    suppliers.merge(partsupp, left_on="S_SUPPKEY", right_on="PS_SUPPKEY")
    .merge(part, left_on="PS_PARTKEY", right_on="P_PARTKEY")
    .merge(nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
    .merge(region, left_on="N_REGIONKEY", right_on="R_REGIONKEY")
)

# Find minimum PS_SUPPLYCOST for each part number
min_cost = result.groupby("P_PARTKEY")["PS_SUPPLYCOST"].min().reset_index()
min_cost_suppliers = result.merge(min_cost, on=["P_PARTKEY", "PS_SUPPLYCOST"])

# Sort by the criteria provided (S_ACCTBAL descending, N_NAME ascending, S_NAME ascending, P_PARTKEY ascending)
final_result = min_cost_suppliers.sort_values(
    by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"],
    ascending=[False, True, True, True]
)

# Select the columns as per the query's request
final_columns = [
    "S_ACCTBAL", "S_NAME", "N_NAME", "P_PARTKEY", "P_MFGR",
    "S_ADDRESS", "S_PHONE", "S_COMMENT"
]
final_result = final_result[final_columns]

# Write to CSV file
final_result.to_csv("query_output.csv", index=False)
