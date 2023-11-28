import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
supplier_collection = mongo_db["supplier"]
partsupp_collection = mongo_db["partsupp"]

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from MongoDB
suppliers_df = pd.DataFrame(list(supplier_collection.find()))
partsupp_df = pd.DataFrame(list(partsupp_collection.find()))

# Retrieve data from Redis
nation_df = pd.DataFrame(redis_client.get('nation'))
region_df = pd.DataFrame(redis_client.get('region'))
part_df = pd.DataFrame(redis_client.get('part'))

# Convert columns to appropriate data types
nation_df['N_NATIONKEY'] = nation_df['N_NATIONKEY'].astype(int)
region_df['R_REGIONKEY'] = region_df['R_REGIONKEY'].astype(int)
part_df['P_SIZE'] = part_df['P_SIZE'].astype(int)

# Filtering and joining data
europe_nations = region_df[region_df.R_NAME == 'EUROPE'].merge(nation_df, left_on='R_REGIONKEY', right_on='N_REGIONKEY')
supplier_europe = suppliers_df.merge(europe_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
part_brass = part_df[(part_df.P_SIZE == 15) & part_df.P_TYPE.str.contains('BRASS')]
supplier_parts = partsupp_df.merge(supplier_europe, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
supplier_parts = supplier_parts.merge(part_brass, left_on='PS_PARTKEY', right_on='P_PARTKEY')

# Selecting the cheapest parts supply cost for each part from a supplier
supplier_parts = supplier_parts.loc[supplier_parts.groupby('PS_PARTKEY')['PS_SUPPLYCOST'].idxmin()]

# Selecting required columns and sorting the data
result = supplier_parts[['S_ACCTBAL', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT', 'P_PARTKEY', 'P_MFGR', 'P_SIZE']]
result = result.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])

# Output the results to CSV
result.to_csv('query_output.csv', index=False)
