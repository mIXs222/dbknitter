import pymongo
import pandas as pd
import direct_redis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client["tpch"]
supplier_collection = mongo_db["supplier"]

# Retrieve suppliers excluding those with 'Customer Complaints' in comments
suppliers_df = pd.DataFrame(list(supplier_collection.find(
    {"S_COMMENT": {"$not": {"$regex": ".*Customer Complaints.*"}}},
    {"S_SUPPKEY": 1, "_id": 0}
)))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)

# Retrieve parts and partsupp dataframe from Redis
part_df = redis_client.get("part")
partsupp_df = redis_client.get("partsupp")

# Exclude unwanted rows based on the criteria
part_df = part_df[
    (part_df['P_BRAND'] != 'Brand#45') &
    (~part_df['P_TYPE'].str.startswith('MEDIUM POLISHED')) &
    (part_df['P_SIZE'].isin([49, 14, 23, 45, 19, 3, 36, 9]))
]

# Merge dataframes to include only relevant supplier keys
merged_df = partsupp_df.merge(part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(suppliers_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Group by brand, type, size and count unique suppliers
grouped_df = merged_df.groupby(['P_BRAND', 'P_TYPE', 'P_SIZE']).agg(SUPPLIER_CNT=('PS_SUPPKEY', 'nunique')).reset_index()

# Sort the results
sorted_df = grouped_df.sort_values(by=['SUPPLIER_CNT', 'P_BRAND', 'P_TYPE', 'P_SIZE'], ascending=[False, True, True, True])

# Write to CSV
sorted_df.to_csv("query_output.csv", index=False)
