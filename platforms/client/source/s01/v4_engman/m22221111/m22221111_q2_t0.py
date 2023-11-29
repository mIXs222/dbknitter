import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
partsupp_collection = mongo_db["partsupp"]

# Retrieve partsupp data from MongoDB
partsupp_data = partsupp_collection.find({})
partsupp_df = pd.DataFrame(list(partsupp_data))

# Connect to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Retrieve data from Redis
nation_df = pd.DataFrame(redis_client.get('nation'))
region_df = pd.DataFrame(redis_client.get('region'))
part_df = pd.DataFrame(redis_client.get('part'))
supplier_df = pd.DataFrame(redis_client.get('supplier'))

# Filter for relevant data
europe_region = region_df[region_df['R_NAME'] == 'EUROPE']
brass_parts = part_df[(part_df['P_TYPE'] == 'BRASS') & (part_df['P_SIZE'] == 15)]

# Merge dataframes
merged_supplier = supplier_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_parts = brass_parts.merge(partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Merge with Europe region
europe_nations = merged_supplier.merge(europe_region, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
final_merged = europe_nations.merge(merged_parts, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Find minimum cost supplier
final_merged['min_cost'] = final_merged.groupby('P_PARTKEY')['PS_SUPPLYCOST'].transform('min')
min_cost_suppliers = final_merged[final_merged['PS_SUPPLYCOST'] == final_merged['min_cost']]

# Sort as mentioned in the query
sorted_suppliers = min_cost_suppliers.sort_values(by=['S_ACCTBAL','N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Select required columns
output_columns = ['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']
output_df = sorted_suppliers[output_columns]

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
