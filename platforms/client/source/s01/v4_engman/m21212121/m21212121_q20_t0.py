# Filename: query_execution.py

import pymongo
import direct_redis
import pandas as pd
from datetime import datetime

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
supplier = pd.DataFrame(list(mongo_db["supplier"].find()))
lineitem = pd.DataFrame(list(mongo_db["lineitem"].find()))

# Prepare the data from MongoDB
lineitem['L_SHIPDATE'] = pd.to_datetime(lineitem['L_SHIPDATE'])
df_mongo = lineitem.merge(supplier, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
df_mongo = df_mongo[(df_mongo['L_SHIPDATE'] >= datetime(1994, 1, 1)) & (df_mongo['L_SHIPDATE'] < datetime(1995, 1, 1))]

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation = pd.read_json(redis_client.get('nation'), orient='records')
part = pd.read_json(redis_client.get('part'), orient='records')
partsupp = pd.read_json(redis_client.get('partsupp'), orient='records')

# Filter for CANADA and Forest Parts
canada_nationkey = nation[nation['N_NAME'] == 'CANADA']['N_NATIONKEY'].values[0]
df_mongo = df_mongo[df_mongo['S_NATIONKEY'] == canada_nationkey]
forest_parts = part[part['P_NAME'].str.contains('forest', case=False, na=False)]

# Merge Redis Data
df_redis = forest_parts.merge(partsupp, how='inner', left_on='P_PARTKEY', right_on='PS_PARTKEY')

# Combine results from both data platforms
final_df = df_mongo.merge(df_redis, how='inner', left_on=['L_PARTKEY', 'L_SUPPKEY'], right_on=['PS_PARTKEY', 'PS_SUPPKEY'])

# Performing the calculation for excess parts
final_df['total_quantity'] = final_df.groupby('L_SUPPKEY')['L_QUANTITY'].transform('sum')
final_df['excess'] = final_df['total_quantity'] > final_df['PS_AVAILQTY'] * 0.5

# Getting the result
result = final_df[final_df['excess']][['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']].drop_duplicates()

# Writing output to CSV
result.to_csv('query_output.csv', index=False)
