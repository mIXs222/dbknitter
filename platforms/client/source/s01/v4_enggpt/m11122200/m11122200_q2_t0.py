import pymongo
import pandas as pd
from pandas.io.json import json_normalize
from direct_redis import DirectRedis

# MongoDB Connection and Query
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
nation = json_normalize(list(mongo_db.nation.find({}, {"_id": 0})))
region = json_normalize(list(mongo_db.region.find({}, {"_id": 0})))
part = json_normalize(list(mongo_db.part.find({"P_SIZE": 15, "P_TYPE": {"$regex": "BRASS"}}, {"_id": 0})))

# Combine MongoDB data
europe_nations = nation.merge(region[region['R_NAME'] == 'EUROPE'], left_on='N_REGIONKEY', right_on='R_REGIONKEY')

# Redis Connection and Query
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier = pd.read_json(redis_client.get('supplier'), orient='records')
partsupp = pd.read_json(redis_client.get('partsupp'), orient='records')

# Combine Redis data
supplier_europe = supplier[supplier['S_NATIONKEY'].isin(europe_nations['N_NATIONKEY'])]

# Merge the data from all sources
final_df = partsupp.merge(supplier_europe, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')
final_df = final_df.merge(part, left_on='PS_PARTKEY', right_on='P_PARTKEY')
final_df = final_df.merge(europe_nations, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Finding the minimum PS_SUPPLYCOST by group
min_cost_df = final_df.groupby(['PS_PARTKEY', 'PS_SUPPKEY']).agg({'PS_SUPPLYCOST': 'min'}).reset_index()
final_df = final_df.merge(min_cost_df, on=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_SUPPLYCOST'])

# Select specified columns to output
output_columns = [
    'S_ACCTBAL', 'N_NAME', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_COMMENT',
    'P_PARTKEY', 'P_MFGR', 'P_SIZE', 'PS_SUPPLYCOST'
]
final_df = final_df.sort_values(by=['S_ACCTBAL', 'N_NAME', 'S_NAME', 'P_PARTKEY'], ascending=[False, True, True, True])
final_df = final_df[output_columns]

# Save to CSV
final_df.to_csv('query_output.csv', index=False)
