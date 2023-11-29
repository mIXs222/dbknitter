# Required Imports
import pymongo
import direct_redis
import pandas as pd
import re

# MongoDB Connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]
lineitem_collection = mongodb["lineitem"]

# Redis Connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Getting data from Redis - Decode bytes to string
part_data_str = r.get('part').decode('utf-8')

# Using regex to find the tuples and strip the unwanted characters
part_tuples = re.findall(r'\(([^\)]+)\)', part_data_str)
part_rows = []

for t in part_tuples:
    row = t.split(',')
    part_rows.append([e.strip("' ") for e in row])

# Columns for part DataFrame
part_columns = ["P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"]

# Create DataFrame for part
df_part = pd.DataFrame(part_rows, columns=part_columns)

# Convert size to integer for filtering
df_part["P_SIZE"] = df_part["P_SIZE"].astype(int)

# Filter part based on the conditions
type1_filter = (df_part["P_BRAND"] == "Brand#12") & (df_part["P_CONTAINER"].isin(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])) & (df_part["P_SIZE"].between(1, 5))
type2_filter = (df_part["P_BRAND"] == "Brand#23") & (df_part["P_CONTAINER"].isin(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])) & (df_part["P_SIZE"].between(1, 10))
type3_filter = (df_part["P_BRAND"] == "Brand#34") & (df_part["P_CONTAINER"].isin(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])) & (df_part["P_SIZE"].between(1, 15))

selected_parts = df_part[type1_filter | type2_filter | type3_filter]

# MongoDB Query
shipmode_conditions = {"L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}}
lineitem_cursor = lineitem_collection.find(shipmode_conditions, {"_id": 0, "L_PARTKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1})

lineitems = list(lineitem_cursor)

# DataFrame for lineitem
df_lineitem = pd.DataFrame(lineitems)

# Filter lineitem based on part keys and quantity
df_lineitem["L_PARTKEY"] = df_lineitem["L_PARTKEY"].astype(int)
lineitem_filter = df_lineitem["L_PARTKEY"].isin(selected_parts["P_PARTKEY"].astype(int))

filtered_lineitem = df_lineitem[lineitem_filter]

# Calculate revenue
filtered_lineitem["REVENUE"] = filtered_lineitem["L_EXTENDEDPRICE"] * (1 - filtered_lineitem["L_DISCOUNT"])

# Group the result and calculate sum
result = filtered_lineitem[["REVENUE"]].sum()

# Write result to csv
result.to_csv('query_output.csv', index=False)
