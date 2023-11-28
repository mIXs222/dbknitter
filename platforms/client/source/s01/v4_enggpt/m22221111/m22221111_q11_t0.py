from pymongo import MongoClient
import pandas as pd
import redis
import direct_redis

# MongoDB connection
client = MongoClient('mongodb', 27017)
mongodb = client["tpch"]
partsupp = pd.DataFrame(list(mongodb.partsupp.find()), columns=['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.read_json(r.get('nation'))
supplier_df = pd.read_json(r.get('supplier'))

# Filtering suppliers from Germany
german_suppliers = supplier_df[supplier_df['S_NATIONKEY'] == nation_df[nation_df['N_NAME'] == 'GERMANY']['N_NATIONKEY'].iloc[0]]

# Joining partsupp and supplier dataframes
merged_df = partsupp.merge(german_suppliers, how='inner', left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculating the value for each part
merged_df['VALUE'] = merged_df['PS_SUPPLYCOST'] * merged_df['PS_AVAILQTY']

# Determining the threshold value
total_value = merged_df['VALUE'].sum()
threshold_percent = 0.05  # Replace with the actual desired percentage
threshold_value = total_value * threshold_percent

# Filtering the parts that exceed the threshold value
filtered_parts = merged_df.groupby('PS_PARTKEY').filter(lambda x: x['VALUE'].sum() > threshold_value)

# Grouping by part and summing the value of each group
grouped_parts = filtered_parts.groupby('PS_PARTKEY').agg({'VALUE': 'sum'}).reset_index()

# Sorting the results in descending order based on the calculated value
final_results = grouped_parts.sort_values(by='VALUE', ascending=False)

# Writing the results to CSV
final_results.to_csv('query_output.csv', index=False)
