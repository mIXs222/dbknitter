# Python code (execute_query.py)

import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Function to convert MongoDB documents into a Pandas DataFrame
def mongodb_to_dataframe(db, collection_name, query, projection):
    collection = db[collection_name]
    cursor = collection.find(query, projection)
    return pd.DataFrame(list(cursor))

# Function to convert Redis keys into a Pandas DataFrame
def redis_to_dataframe(redis_client, key_pattern):
    keys = redis_client.keys(key_pattern)
    data = []
    for key in keys:
        data.append(redis_client.hgetall(key))
    return pd.DataFrame(data)

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# MongoDB queries
supplier_df = mongodb_to_dataframe(mongodb, "supplier", {}, {'_id': 0})
lineitem_df = mongodb_to_dataframe(mongodb, "lineitem", {'L_PARTKEY': {'$regex': '.*dim.*'}}, {'_id': 0})

# Redis queries
nation_df = redis_to_dataframe(redis_client, 'nation:*')
part_df = redis_to_dataframe(redis_client, 'part:*')
partsupp_df = redis_to_dataframe(redis_client, 'partsupp:*')
orders_df = redis_to_dataframe(redis_client, 'orders:*')

# Convert columns to appropriate data types
nation_df = nation_df.astype({'N_NATIONKEY': 'int32'})
part_df = part_df[part_df['P_NAME'].str.contains('dim')]
partsupp_df = partsupp_df.astype({'PS_PARTKEY': 'int32', 'PS_SUPPKEY': 'int32'})
orders_df = orders_df.astype({'O_ORDERKEY': 'int32', 'O_ORDERDATE': 'datetime64'})

# Merge DataFrames
merged_df = (
    supplier_df.merge(lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
    .merge(partsupp_df, left_on=['L_SUPPKEY', 'L_PARTKEY'], right_on=['PS_SUPPKEY', 'PS_PARTKEY'])
    .merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')
    .merge(orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
    .merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
)

# Calculate profit and year
merged_df['AMOUNT'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT']) - merged_df['PS_SUPPLYCOST'] * merged_df['L_QUANTITY']
merged_df['O_YEAR'] = merged_df['O_ORDERDATE'].dt.year

# Perform the aggregation
result_df = (
    merged_df.groupby(['N_NAME', 'O_YEAR'])
    .agg({'AMOUNT': 'sum'})
    .reset_index()
    .rename(columns={'N_NAME': 'NATION', 'AMOUNT': 'SUM_PROFIT'})
)

# Sort the results
result_df.sort_values(by=['NATION', 'O_YEAR'], ascending=[True, False], inplace=True)

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
