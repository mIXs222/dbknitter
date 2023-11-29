import pymongo
from pymongo import MongoClient
import direct_redis
import pandas as pd

# MongoDB connection
mongo_client = MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
nation = mongo_db["nation"]
region = mongo_db["region"]
part = mongo_db["part"]
supplier = mongo_db["supplier"]

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get relevant data from redis
partsupp_df = pd.read_msgpack(redis_client.get('partsupp'))

# Query for MongoDB and Redis data
europe_regions = list(region.find({"R_NAME": "EUROPE"}))
europe_regions_keys = [region['R_REGIONKEY'] for region in europe_regions]

europe_nations = list(nation.find({"N_REGIONKEY": {"$in": europe_regions_keys}}))
europe_nations_keys = [nation['N_NATIONKEY'] for nation in europe_nations]

europe_suppliers = list(supplier.find({"S_NATIONKEY": {"$in": europe_nations_keys}}))
supplier_keys = [sup['S_SUPPKEY'] for sup in europe_suppliers]

brass_parts = list(part.find({"P_TYPE": "BRASS", "P_SIZE": 15}))
part_keys = [part['P_PARTKEY'] for part in brass_parts]

# Create DataFrame for MongoDB collections
suppliers_df = pd.DataFrame(europe_suppliers)
nations_df = pd.DataFrame(europe_nations)
parts_df = pd.DataFrame(brass_parts)

# Filter partsupp for relevant data
partsupp_df = partsupp_df[(partsupp_df['PS_SUPPKEY'].isin(supplier_keys)) & (partsupp_df['PS_PARTKEY'].isin(part_keys))]

# Find minimum cost per part
min_cost_df = partsupp_df.loc[partsupp_df.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].idxmin()]

# Merge all data together
result_df = pd.merge(min_cost_df, suppliers_df, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
result_df = pd.merge(result_df, nations_df, how='inner', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
result_df = pd.merge(result_df, parts_df, how='inner', left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Sort as per query requirements
result_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Select required columns
result_df = result_df[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]

# Write the result to CSV
result_df.to_csv('query_output.csv', index=False)
