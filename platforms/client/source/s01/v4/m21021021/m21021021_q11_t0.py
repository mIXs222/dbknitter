import pymongo
import pandas as pd
from pandarallel import pandarallel
import direct_redis

# Initialize parallel computing
pandarallel.initialize()

# MongoDB connection
client = pymongo.MongoClient("mongodb", 27017)
db = client['tpch']
partsupp_collection = db['partsupp']
partsupp_data = pd.DataFrame(list(partsupp_collection.find({}, {'_id': 0})))

# Redis connection
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_data = redis_connection.get('nation')
supplier_data = redis_connection.get('supplier')

# Filter the given datasets for 'GERMANY'
nation_germany = nation_data[nation_data['N_NAME'] == 'GERMANY']
supplier_nation_key = supplier_data[supplier_data['S_SUPPKEY'].isin(nation_germany['N_NATIONKEY'])]

# Merge the datasets
merged_df = partsupp_data.merge(supplier_nation_key, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculate the SUM(PS_SUPPLYCOST * PS_AVAILQTY) and filter by 'GERMANY'
merged_df['VALUE'] = merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']
grouped = merged_df.groupby('PS_PARTKEY').agg({'VALUE': ['sum']})
grouped.columns = grouped.columns.droplevel(1)

# Filter based on HAVING condition
total_value_sum = grouped['VALUE'].sum() * 0.0001000000
filtered_group = grouped[grouped['VALUE'] > total_value_sum].reset_index()

# Sort by VALUE DESC and output to CSV
filtered_group = filtered_group.sort_values(by='VALUE', ascending=False)
filtered_group.to_csv('query_output.csv', index=False)
