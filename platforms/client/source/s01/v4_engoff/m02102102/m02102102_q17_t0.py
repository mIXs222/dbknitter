from pymongo import MongoClient
import pandas as pd
import direct_redis

# Connect to MongoDB
mongodb_client = MongoClient('mongodb', 27017)
mongodb = mongodb_client['tpch']
part_collection = mongodb['part']

# Query MongoDB for part data
part_query = {"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"}
part_fields = {"P_PARTKEY": 1, "_id": 0}
part_df = pd.DataFrame(list(part_collection.find(part_query, part_fields)))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379)
lineitem_data = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Merge datasets
merged_df = lineitem_df.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filter based on the criteria and calculate the average quantity and revenue
filtered_df = merged_df[merged_df['L_QUANTITY'] < merged_df['L_QUANTITY'].mean() * 0.2]
avg_yearly_revenue_loss = filtered_df['L_EXTENDEDPRICE'].sum() / 7

# Write to CSV file
pd.DataFrame({'avg_yearly_revenue_loss': [avg_yearly_revenue_loss]}).to_csv('query_output.csv', index=False)
