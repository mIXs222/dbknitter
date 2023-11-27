# import the required libraries
from pymongo import MongoClient
import pandas as pd
import redis
import direct_redis

# MongoDB connection data
mongo_hostname = 'mongodb'
mongo_port = 27017
mongo_db_name = 'tpch'

# Redis connection data
redis_hostname = 'redis'
redis_port = 6379
redis_db_name = '0'

# Connect to MongoDB
mongo_client = MongoClient(host=mongo_hostname, port=mongo_port)
mongo_db = mongo_client[mongo_db_name]

# Query MongoDB for nation, region, part, and supplier
nation_df = pd.DataFrame(list(mongo_db.nation.find()))
region_df = pd.DataFrame(list(mongo_db.region.find()))
part_df = pd.DataFrame(list(mongo_db.part.find({'P_TYPE': 'BRASS', 'P_SIZE': 15})))
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host=redis_hostname, port=redis_port, db=redis_db_name)

# Query Redis for partsupp
partsupp_df_raw = redis_client.get('partsupp')
partsupp_df = pd.read_json(partsupp_df_raw)

# Merge and filter the dataframes accordingly
merged_df = partsupp_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(part_df, left_on='PS_PARTKEY', right_on='P_PARTKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter for EUROPE region
europe_df = merged_df[merged_df['R_NAME'] == 'EUROPE']

# Find the minimum cost per part
europe_df['PS_SUPPLYCOST'] = pd.to_numeric(europe_df['PS_SUPPLYCOST'])
min_cost_df = europe_df.loc[europe_df.groupby('P_PARTKEY')['PS_SUPPLYCOST'].idxmin()]

# Order by the specified columns
ordered_df = min_cost_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Selected Output Columns
output_columns = [
    'S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR',
    'S_ADDRESS', 'S_PHONE', 'S_COMMENT'
]
final_output = ordered_df[output_columns]

# Write to CSV
final_output.to_csv('query_output.csv', index=False)
