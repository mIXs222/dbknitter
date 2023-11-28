import pymongo
import pandas as pd
import direct_redis

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
supplier_df = pd.DataFrame(list(mongodb.supplier.find({'S_NATIONKEY': 'GERMANY'})))
partsupp_df = pd.DataFrame(list(mongodb.partsupp.find()))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(redis_client.get('nation'), orient='records')

# Merge tables based on keys
merged_df = supplier_df.merge(partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')
merged_df = merged_df.merge(nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Calculate the sum of the supply cost multiplied by the available quantity
merged_df['total_value'] = merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']

# Calculate the threshold
threshold = merged_df['total_value'].sum() * 0.05  # Assuming 5% threshold for illustration

# Filter based on having condition
result_df = merged_df.groupby('PS_PARTKEY').filter(lambda x: x['total_value'].sum() > threshold)

# Group by part key, aggregate values, and sort
final_df = (result_df.groupby('PS_PARTKEY')
            .agg(total_value=('total_value', 'sum'))
            .reset_index()
            .sort_values('total_value', ascending=False))

# Write output to CSV
final_df.to_csv('query_output.csv', index=False)
