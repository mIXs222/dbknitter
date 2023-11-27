import pymongo
import redis
import pandas as pd
from direct_redis import DirectRedis

# MongoDB Connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]
part_col = mongodb["part"]
supplier_col = mongodb["supplier"]

# Redis Connection
r = DirectRedis(host="redis", port=6379, db=0)

# Fetch data from MongoDB
part_data = pd.DataFrame(list(part_col.find(
    {"$and": [
        {"P_BRAND": {"$ne": "Brand#45"}},
        {"P_TYPE": {"$not": {"$regex": "MEDIUM POLISHED.*"}}},
        {"P_SIZE": {"$in": [49, 14, 23, 45, 19, 3, 36, 9]}}
    ]}
)))

supplier_data = pd.DataFrame(list(supplier_col.find(
    {"S_COMMENT": {"$not": {"$regex": ".*Customer.*Complaints.*"}}}
)))

# Get part-supplier relationship data from Redis
partsupp_data_encoded = r.get("partsupp")
partsupp_data_json = partsupp_data_encoded.decode('utf-8')
partsupp_data = pd.read_json(partsupp_data_json, orient='records')

# Merge the datasets
result = part_data.merge(
    partsupp_data, left_on="P_PARTKEY", right_on="PS_PARTKEY")

result = result[~result["PS_SUPPKEY"].isin(supplier_data["S_SUPPKEY"])]

# Perform grouping and counting
grouped_result = result.groupby(["P_BRAND", "P_TYPE", "P_SIZE"]).agg(SUPPLIER_CNT=pd.NamedAgg(column="PS_SUPPKEY", aggfunc="nunique")).reset_index()

# Sort the result
sorted_result = grouped_result.sort_values(by=["SUPPLIER_CNT", "P_BRAND", "P_TYPE", "P_SIZE"], ascending=[False, True, True, True])

# Save result to csv file
sorted_result.to_csv("query_output.csv", index=False)
