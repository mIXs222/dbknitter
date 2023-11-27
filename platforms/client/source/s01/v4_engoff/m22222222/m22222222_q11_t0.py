import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to Redis
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0
r = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Load data from Redis
nation_df = pd.read_json(r.get('nation'))
supplier_df = pd.read_json(r.get('supplier'))
partsupp_df = pd.read_json(r.get('partsupp'))

# Filter suppliers from Germany
filtered_nation = nation_df[nation_df['N_NAME'] == 'GERMANY']
german_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(filtered_nation['N_NATIONKEY'])]

# Join tables to gather necessary information
joined_data = german_suppliers.merge(partsupp_df, left_on='S_SUPPKEY', right_on='PS_SUPPKEY')

# Compute total value of available parts
joined_data['TOTAL_VALUE'] = joined_data['PS_AVAILQTY'] * joined_data['PS_SUPPLYCOST']

# Find all parts that represent a significant percentage of the total value of all available parts
total_value_sum = joined_data['TOTAL_VALUE'].sum()
joined_data['VALUE_RATIO'] = joined_data['TOTAL_VALUE'] / total_value_sum
important_parts = joined_data[joined_data['VALUE_RATIO'] > 0.0001]

# Select relevant columns and sort by descending order of value
output_data = important_parts[['PS_PARTKEY', 'TOTAL_VALUE']].sort_values(by='TOTAL_VALUE', ascending=False)

# Write results to CSV file
output_data.to_csv('query_output.csv', index=False)

print("Query executed and output saved to query_output.csv")
