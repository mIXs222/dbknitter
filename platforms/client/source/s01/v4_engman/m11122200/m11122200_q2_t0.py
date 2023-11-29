import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_nation = mongo_db["nation"].find({}, {"_id": 0})
mongo_region = mongo_db["region"].find({}, {"_id": 0})
mongo_part = mongo_db["part"].find(
    {"P_TYPE": "BRASS", "P_SIZE": 15}, {"_id": 0}
)

# Convert to Pandas DataFrame
df_nation = pd.DataFrame(list(mongo_nation))
df_region = pd.DataFrame(list(mongo_region))
df_part = pd.DataFrame(list(mongo_part))

# Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis as DataFrame
df_supplier = pd.read_json(redis_client.get('supplier'))
df_partsupp = pd.read_json(redis_client.get('partsupp'))

# Join and filter data
df_europe_nations = df_nation[df_nation["N_REGIONKEY"].isin(df_region[df_region["R_NAME"] == "EUROPE"]["R_REGIONKEY"].tolist())]
df_supplier = df_supplier[df_supplier["S_NATIONKEY"].isin(df_europe_nations["N_NATIONKEY"].tolist())]

# Merge all dataframes to get required data
merged_df = df_partsupp.merge(
    df_supplier, left_on="PS_SUPPKEY", right_on="S_SUPPKEY"
).merge(
    df_nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY"
).merge(
    df_part, left_on="PS_PARTKEY", right_on="P_PARTKEY"
)

# Sort by the specified order and extract necessary columns
result_df = merged_df.sort_values(
    by=["PS_SUPPLYCOST", "S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"],
    ascending=[True, False, True, True, True]
)

# Filter out the lowest supply cost only
result_df = result_df.groupby('P_PARTKEY').head(1)

# Select and order columns as specified
output_df = result_df[[
    "N_NAME", "P_MFGR", "P_PARTKEY", "S_ACCTBAL", "S_ADDRESS", 
    "S_COMMENT", "S_NAME", "S_PHONE"
]]

# Write to CSV
output_df.to_csv("query_output.csv", index=False)
