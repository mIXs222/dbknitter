# query.py
from pymongo import MongoClient
import direct_redis
import pandas as pd

# MongoDB Connection Setup
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Redis Connection Setup
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MongoDB collections
nation_df = pd.DataFrame(list(mongodb.nation.find({}, {'_id': 0})))
region_df = pd.DataFrame(list(mongodb.region.find({}, {'_id': 0})))
part_df = pd.DataFrame(list(mongodb.part.find({'P_SIZE': 15, 'P_TYPE': {'$regex': 'BRASS$'}}, {'_id': 0})))
supplier_df = pd.DataFrame(list(mongodb.supplier.find({}, {'_id': 0})))

# Fetch data from Redis
partsupp_df = pd.DataFrame(eval(redis_client.get('partsupp')))

# Merge and filter the data frames as per the SQL query conditions
# We perform the filtering and merging step by step.
merged_df = pd.merge(part_df, partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
merged_df = pd.merge(merged_df, supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged_df = pd.merge(merged_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = pd.merge(merged_df, region_df, left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Filter rows for region 'EUROPE'
merged_df = merged_df[merged_df['R_NAME'] == 'EUROPE']

# Find the minimum PS_SUPPLYCOST for region 'EUROPE'
min_supply_cost_europe = merged_df['PS_SUPPLYCOST'].min()

# Further filter the data to use only the parts with the minimum supply cost.
filtered_df = merged_df[merged_df['PS_SUPPLYCOST'] == min_supply_cost_europe]

# Select and order the final columns
result_df = filtered_df[[
    'S_ACCTBAL',
    'S_NAME',
    'N_NAME',
    'P_PARTKEY',
    'P_MFGR',
    'S_ADDRESS',
    'S_PHONE',
    'S_COMMENT'
]].sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Save the result to csv
result_df.to_csv('query_output.csv', index=False)
