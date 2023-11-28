# Python Code: query.py
from pymongo import MongoClient
import pandas as pd
import redis
import direct_redis

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Retrieve relevant data from MongoDB
nation = pd.DataFrame(list(db.nation.find({}, {'_id': 0})))
suppliers = pd.DataFrame(list(db.supplier.find({}, {'_id': 0})))

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'partsupp' data from Redis and parse it as a DataFrame
partsupp_data = r.get('partsupp')
partsupp_df = pd.read_json(partsupp_data)

# Filter for German suppliers
german_suppliers = suppliers[suppliers['N_NATIONKEY'] == nation[nation['N_NAME'] == 'GERMANY']['N_NATIONKEY'].values[0]]

# Join suppliers with partsupp to consider only parts supplied by German suppliers
german_partsupp = german_suppliers.merge(partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Calculate the total value for each part by supplier
german_partsupp['VALUE'] = german_partsupp['PS_SUPPLYCOST'] * german_partsupp['PS_AVAILQTY']

# Calculate the total value for parts from German suppliers
total_value_germany = german_partsupp['VALUE'].sum()

# Group by part key and filter based on the percentage threshold
threshold_percentage = 0.05  # Placeholder for percentage threshold
threshold_value = total_value_germany * threshold_percentage

grouped_partsupp = german_partsupp.groupby('PS_PARTKEY').agg({'VALUE': 'sum'})
filtered_parts = grouped_partsupp[grouped_partsupp['VALUE'] > threshold_value]

# Sort the final results
sorted_parts = filtered_parts.sort_values(by='VALUE', ascending=False)

# Write to CSV
sorted_parts.to_csv('query_output.csv')
