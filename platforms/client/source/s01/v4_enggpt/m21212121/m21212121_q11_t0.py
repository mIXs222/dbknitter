from pymongo import MongoClient
import pandas as pd
import json

# Connection to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']

# Connection to Redis
import direct_redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379)

# Fetch data from MongoDB
supplier_df = pd.DataFrame(list(supplier_collection.find({}, {'_id': 0})))

# Fetch data from Redis and convert JSON to DataFrame
nation_data = json.loads(redis_client.get('nation'))
partsupp_data = json.loads(redis_client.get('partsupp'))

nation_df = pd.DataFrame(nation_data)
partsupp_df = pd.DataFrame(partsupp_data)

# Merge dataframes
merged_df = pd.merge(supplier_df, nation_df, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merged_df = pd.merge(merged_df, partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Filter suppliers located in Germany
germany_suppliers = merged_df[merged_df['N_NAME'] == 'GERMANY']

# Calculate total value for each part
germany_suppliers['TOTAL_VALUE'] = germany_suppliers['PS_SUPPLYCOST'] * germany_suppliers['PS_AVAILQTY']

# Compute the threshold for Germany-based suppliers
threshold_value = germany_suppliers['TOTAL_VALUE'].sum() * 0.01  # Assuming the threshold is 1% of total value

# Group by part key
grouped_data = germany_suppliers.groupby('PS_PARTKEY').agg(
    TOTAL_VALUE_GROUPED=('TOTAL_VALUE', 'sum')
)
filtered_parts = grouped_data[grouped_data['TOTAL_VALUE_GROUPED'] > threshold_value]

# Sort the values and write to CSV
final_output = filtered_parts.sort_values('TOTAL_VALUE_GROUPED', ascending=False)
final_output.to_csv('query_output.csv')
