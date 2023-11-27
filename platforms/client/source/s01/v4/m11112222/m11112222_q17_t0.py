from pymongo import MongoClient
import pandas as pd
import direct_redis

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Querying MongoDB for part data
part_query = {'P_BRAND': 'Brand#23', 'P_CONTAINER': 'MED BAG'}
part_projection = {'_id': 0, 'P_PARTKEY': 1}
part_df = pd.DataFrame(list(part_collection.find(part_query, part_projection)))

# Load lineitem from Redis into a DataFrame
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))

# Filtering lineitem DataFrame based on part keys from the part collection.
lineitem_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(part_df['P_PARTKEY'])]

# Calculate the average quantity for each part
avg_qty = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_qty.rename(columns={'L_QUANTITY': 'AVG_QTY'}, inplace=True)

# Merge with the original lineitem DataFrame to use the average quantity in the WHERE clause
lineitem_df = lineitem_df.merge(avg_qty, on='L_PARTKEY')
lineitem_df = lineitem_df[lineitem_df['L_QUANTITY'] < 0.2 * lineitem_df['AVG_QTY']]

# Calculate the sum of L_EXTENDEDPRICE
avg_yearly = lineitem_df['L_EXTENDEDPRICE'].sum() / 7.0

# Save the result to CSV
result_df = pd.DataFrame({'AVG_YEARLY': [avg_yearly]})
result_df.to_csv('query_output.csv', index=False)
