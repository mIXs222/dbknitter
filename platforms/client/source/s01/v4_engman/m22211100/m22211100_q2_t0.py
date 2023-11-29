import pymongo
import pandas as pd
from direct_redis import DirectRedis
import redis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
supplier_col = mongodb["supplier"]
partsupp_col = mongodb["partsupp"]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Load redis data into Pandas DataFrames
nation_df = pd.read_json(redis_client.get('nation'))
region_df = pd.read_json(redis_client.get('region'))
part_df = pd.read_json(redis_client.get('part'))

# Query to find Europe region key
europe_key = region_df[region_df['R_NAME'] == "EUROPE"]['R_REGIONKEY'].values[0]

# Select nations that belong to Europe
european_nations = nation_df[nation_df['N_REGIONKEY'] == europe_key]

# Filter parts for BRASS type and size 15
brass_parts = part_df[(part_df['P_TYPE'] == "BRASS") & (part_df['P_SIZE'] == 15)]

# Fetch supplier and partsupp data from MongoDB
supplier_data = list(supplier_col.find())
partsupp_data = list(partsupp_col.find())

# Convert MongoDB data into Pandas DataFrames
supplier_df = pd.DataFrame(supplier_data)
partsupp_df = pd.DataFrame(partsupp_data)

# Merge dataframes to combine information
merged_df = partsupp_df.merge(supplier_df, how='left', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(european_nations, how='left', left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = merged_df.merge(brass_parts, how='left', left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Find minimum cost for each part by supplier
merged_df['min_cost'] = merged_df.groupby('P_PARTKEY')['PS_SUPPLYCOST'].transform(min)
min_cost_suppliers = merged_df[merged_df['PS_SUPPLYCOST'] == merged_df['min_cost']]

# Order the result as per the query instructions
ordered_result = min_cost_suppliers.sort_values(
    by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'],
    ascending=[False, True, True, True]
)

# Select the desired columns
final_result = ordered_result[[
    'N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL',
    'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE'
]]

# Write result to CSV file
final_result.to_csv('query_output.csv', index=False)
