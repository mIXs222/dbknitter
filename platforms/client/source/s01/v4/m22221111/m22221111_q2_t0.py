# query.py
import pymongo
import direct_redis
import pandas as pd

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
partsupp_collection = mongodb["partsupp"]

partsupp_df = pd.DataFrame(list(partsupp_collection.find()))

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_msgpack(r.get('nation'))
region_df = pd.read_msgpack(r.get('region'))
part_df = pd.read_msgpack(r.get('part'))
supplier_df = pd.read_msgpack(r.get('supplier'))

# Filter by region name
eu_region_keys = region_df[region_df['R_NAME'] == 'EUROPE']['R_REGIONKEY']
eu_nations = nation_df[nation_df['N_REGIONKEY'].isin(eu_region_keys)]

# Join dataframes
result_df = part_df.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
result_df = result_df.merge(supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
result_df = result_df.merge(eu_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Filter by size, type and nested select
result_df = result_df[(result_df['P_SIZE'] == 15) & (result_df['P_TYPE'].str.endswith('BRASS'))]
min_cost_df = result_df.groupby('P_PARTKEY')['PS_SUPPLYCOST'].min().reset_index()
result_df = result_df.merge(min_cost_df, on=['P_PARTKEY', 'PS_SUPPLYCOST'])

# Select specific columns and sort
result_df = result_df[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY',
                       'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']].sort_values(
    by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Save to CSV
result_df.to_csv('query_output.csv', index=False)
