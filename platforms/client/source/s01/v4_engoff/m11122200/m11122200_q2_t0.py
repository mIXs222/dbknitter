from pymongo import MongoClient
import direct_redis
import pandas as pd

# Connect to the MongoDB database
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
nation_col = mongo_db['nation']
region_col = mongo_db['region']
part_col = mongo_db['part']

# Fetch required data from MongoDB
europe_regions = list(region_col.find({"R_NAME": "EUROPE"}, {"_id": 0, "R_REGIONKEY": 1}))
europe_region_keys = [r['R_REGIONKEY'] for r in europe_regions]
european_nations = list(nation_col.find({"N_REGIONKEY": {"$in": europe_region_keys}}, {"_id": 0}))
df_european_nations = pd.DataFrame(european_nations)

# Connect to the Redis database
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch required data from Redis
df_supplier = pd.DataFrame(redis_client.get('supplier'))
df_partsupp = pd.DataFrame(redis_client.get('partsupp'))

# Fetch required data from MongoDB
brass_parts = list(part_col.find({"P_TYPE": "BRASS", "P_SIZE": 15}, {"_id": 0}))
df_brass_parts = pd.DataFrame(brass_parts)

# Merge DataFrames to get the necessary information
df_european_suppliers = pd.merge(df_supplier, df_european_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df_european_supplier_parts = pd.merge(df_partsupp, df_european_suppliers, left_on='S_SUPPKEY', right_on='S_SUPPKEY')
df_brass_parts_suppliers = pd.merge(df_european_supplier_parts, df_brass_parts, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Calculate minimum cost suppliers
df_brass_parts_suppliers['min_cost'] = df_brass_parts_suppliers.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].transform('min')
df_min_cost_suppliers = df_brass_parts_suppliers[df_brass_parts_suppliers['PS_SUPPLYCOST'] == df_brass_parts_suppliers['min_cost']]

# Sort the results
df_sorted = df_min_cost_suppliers.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Select required columns
df_result = df_sorted[['S_ACCTBAL', 'S_NAME', 'N_NAME', 'P_PARTKEY', 'P_MFGR', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT']]

# Write to CSV
df_result.to_csv('query_output.csv', index=False)
