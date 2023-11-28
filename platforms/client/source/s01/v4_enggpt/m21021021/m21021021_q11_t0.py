import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
parts_collection = mongo_db["partsupp"]
parts_df = pd.DataFrame(list(parts_collection.find()))

# Redis Connection
redis_client = DirectRedis(host='redis', port=6379, db=0)
supplier_df = pd.read_msgpack(redis_client.get('supplier'))
nation_df = pd.read_msgpack(redis_client.get('nation'))

# Merge and Filter Data
supplier_df = supplier_df[supplier_df['S_NATIONKEY'] == nation_df[nation_df['N_NAME'] == 'GERMANY'].iloc[0]['N_NATIONKEY']]
combined_df = pd.merge(parts_df, supplier_df, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Apply Transformation and Filtering
combined_df['TOTAL_VALUE'] = combined_df['PS_SUPPLYCOST'] * combined_df['PS_AVAILQTY']
value_threshold = combined_df['TOTAL_VALUE'].sum() * 0.05  # Suppose the threshold is 5% of the overall value

# Group by PS_PARTKEY and Filter
grouped_df = combined_df.groupby('PS_PARTKEY').agg({'TOTAL_VALUE': 'sum'}).reset_index()
filtered_df = grouped_df[grouped_df['TOTAL_VALUE'] > value_threshold]

# Sort the values in descending order
sorted_df = filtered_df.sort_values('TOTAL_VALUE', ascending=False)

# Write to CSV
sorted_df.to_csv('query_output.csv', index=False)
