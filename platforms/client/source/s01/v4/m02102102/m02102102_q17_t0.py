# mongodb_query.py
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]
part_collection = mongodb["part"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query MongoDB for part data with specified brand and container
part_df = pd.DataFrame(list(part_collection.find({"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"},
                                                 {"_id": 0, "P_PARTKEY": 1})))

# Query Redis for lineitem data and load it as a DataFrame
lineitem_df = pd.read_pickle(redis_client.get("lineitem"))

# Merge lineitem and part dataframes on partkey
merged_df = lineitem_df.merge(part_df, left_on="L_PARTKEY", right_on="P_PARTKEY")

# Group by partkey to calculate average quantity for each part
avg_quantity_per_part = merged_df.groupby('P_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_quantity_per_part['avg_quantity'] = avg_quantity_per_part['L_QUANTITY'] * 0.2
avg_quantity_per_part.drop('L_QUANTITY', axis=1, inplace=True)

# Filter the merged dataframe for lineitems with less quantity than 0.2 times their part's average quantity
filtered_lineitems = merged_df.merge(avg_quantity_per_part, on='P_PARTKEY')
filtered_lineitems = filtered_lineitems[filtered_lineitems['L_QUANTITY'] < filtered_lineitems['avg_quantity']]

# Calculate the required sum of extended price divided by 7
avg_yearly = filtered_lineitems['L_EXTENDEDPRICE'].sum() / 7.0

# Write result to 'query_output.csv'
pd.DataFrame({"AVG_YEARLY": [avg_yearly]}).to_csv("query_output.csv", index=False)
