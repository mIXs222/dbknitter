from pymongo import MongoClient
import pandas as pd
import direct_redis
import csv

# Establish MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']

# Select the required collections in MongoDB
nation_col = mongodb['nation']
part_col = mongodb['part']
partsupp_col = mongodb['partsupp']

# Fetch documents from MongoDB and create DataFrames
df_nation = pd.DataFrame(list(nation_col.find({}, {'_id': 0})))
df_part = pd.DataFrame(list(part_col.find({'P_TYPE': 'BRASS', 'P_SIZE': 15}, {'_id': 0})))
df_partsupp = pd.DataFrame(list(partsupp_col.find({}, {'_id': 0})))

# Establish Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch data from Redis and create DataFrames
region = redis_client.get('region')
df_region = pd.read_json(region)

supplier = redis_client.get('supplier')
df_supplier = pd.read_json(supplier)

# Filter out the region EUROPE
df_region_europe = df_region[df_region['R_NAME'] == 'EUROPE']

# Merge DataFrames to get relevant information
df_merged = df_partsupp.merge(df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
df_merged = df_merged.merge(df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
df_merged = df_merged.merge(df_region_europe, left_on='N_REGIONKEY', right_on='R_REGIONKEY')
df_merged = df_merged.merge(df_part, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Get suppliers with the minimum cost offering the BRASS part size 15 in the EUROPE region
df_min_cost = df_merged.groupby('P_PARTKEY').apply(lambda x: x[x['PS_SUPPLYCOST'] == x['PS_SUPPLYCOST'].min()])
df_min_cost.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Select columns according to the order specified in the query
output_columns = ['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']
final_output = df_min_cost[output_columns]

# Write output to CSV file
final_output.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
