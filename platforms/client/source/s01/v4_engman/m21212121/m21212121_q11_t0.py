import pymongo
import direct_redis
import pandas as pd

# MongoDB connection details
MONGO_HOST = 'mongodb'
MONGO_PORT = 27017
MONGO_DB = 'tpch'

# Redis connection details
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_DB = 0

# MongoDB connection and query
mongo_client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
mongodb = mongo_client[MONGO_DB]

# Query MongoDB for suppliers in Germany
suppliers_in_germany = list(mongodb.supplier.find({'S_NATIONKEY': {'$eq': 5}}, {'_id': 0}))
df_suppliers = pd.DataFrame(suppliers_in_germany)

# Redis connection and query
redis_client = direct_redis.DirectRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

nation = pd.read_json(redis_client.get('nation'))
partsupp = pd.read_json(redis_client.get('partsupp'))

# Join data frames
merged_df = df_suppliers.merge(nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
final_df = merged_df.merge(partsupp, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Calculate value and filter
final_df['VALUE'] = final_df['PS_SUPPLYCOST'] * final_df['PS_AVAILQTY']
result_df = final_df[final_df['VALUE'] > final_df['VALUE'].sum() * 0.0001]

# Select part number and value, sort by value descending
output_df = result_df[['PS_PARTKEY', 'VALUE']].sort_values('VALUE', ascending=False)

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
