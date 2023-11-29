# product_type_profit_measure.py
import pymongo
from bson.son import SON
import pandas as pd
from direct_redis import DirectRedis
import os

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb", 27017)
mongodb_db = mongodb_client["tpch"]

# Connect to Redis, assuming DirectRedis is similar in usage to redis.Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Define function to query MongoDB
def query_mongodb(collection_name, filter_data={}, projection_data={}):
    return pd.DataFrame(list(mongodb_db[collection_name].find(filter_data, projection_data)))

# Define function to query Redis and convert to DataFrame
def query_redis(table_name):
    data = redis_client.get(table_name) 
    return pd.DataFrame.from_records(data, columns=data[0].keys() if data else None)

# Query MongoDB and Redis tables
lineitem_df = query_mongodb('lineitem', projection_data={'_id': False})
partsupp_df = query_mongodb('partsupp', projection_data={'_id': False})
supplier_df = query_redis('supplier')
nation_df = query_redis('nation')

# Merge DataFrames to calculate profits
merged_df = pd.merge(lineitem_df, partsupp_df, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])
merged_df = pd.merge(merged_df, supplier_df, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = pd.merge(merged_df, nation_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate profit
merged_df['PROFIT'] = (merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])) - (merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY'])

# Extract year from L_SHIPDATE
merged_df['YEAR'] = pd.to_datetime(merged_df['L_SHIPDATE']).dt.year

# Group by nation and year
grouped_df = merged_df.groupby(['N_NAME', 'YEAR']).agg({'PROFIT': 'sum'}).reset_index()

# Sort the result
grouped_df.sort_values(by=['N_NAME', 'YEAR'], ascending=[True, False], inplace=True)

# Write to CSV file
grouped_df.to_csv('query_output.csv', index=False)

# Clean up clients
redis_client.close()
mongodb_client.close()
