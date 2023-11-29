from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_region = pd.DataFrame(list(mongo_db.region.find()))
mongo_supplier = pd.DataFrame(list(mongo_db.supplier.find()))

# Filter regions representing 'EUROPE'
europe_region_keys = mongo_region[mongo_region['R_NAME'] == 'EUROPE']['R_REGIONKEY'].tolist()

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379)
nation_df = pd.read_json(redis_client.get('nation'))
part_df = pd.read_json(redis_client.get('part'))
partsupp_df = pd.read_json(redis_client.get('partsupp'))

# Filter nations in 'EUROPE' region
europe_nations = nation_df[nation_df['N_REGIONKEY'].isin(europe_region_keys)]

# Filter parts of type 'BRASS' and size '15'
brass_parts = part_df[(part_df['P_TYPE'] == 'BRASS') & (part_df['P_SIZE'] == 15)]

# Perform the join operations to get the required result
result = pd.merge(brass_parts, partsupp_df, left_on='P_PARTKEY', right_on='PS_PARTKEY')
result = result.merge(mongo_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
result = result.merge(europe_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Find the minimum supply cost for each part
result['MIN_PS_SUPPLYCOST'] = result.groupby(['P_PARTKEY'])['PS_SUPPLYCOST'].transform(min)
result = result[result['PS_SUPPLYCOST'] == result['MIN_PS_SUPPLYCOST']]

# Order by S_ACCTBAL (DESC), N_NAME, S_NAME, P_PARTKEY (ASC)
result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True], inplace=True)

# Select and rename the desired columns
final_result = result[['N_NAME', 'P_MFGR', 'P_PARTKEY', 'S_ACCTBAL', 'S_ADDRESS', 'S_COMMENT', 'S_NAME', 'S_PHONE']]

# Write to CSV file
final_result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_ALL)
