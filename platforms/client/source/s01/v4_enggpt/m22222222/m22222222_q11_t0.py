# query.py
import pandas as pd
import direct_redis

# Connection information for Redis
redis_hostname = 'redis'
redis_port = 6379
database_name = 0

# Connecting to Redis
redis_db = direct_redis.DirectRedis(host=redis_hostname, port=redis_port, db=database_name)

# Retrieving tables as DataFrames from Redis
nation_df = pd.read_json(redis_db.get('nation'))
supplier_df = pd.read_json(redis_db.get('supplier'))
partsupp_df = pd.read_json(redis_db.get('partsupp'))

# Filtering suppliers located in Germany
suppliers_in_germany = supplier_df.merge(nation_df[nation_df['N_NAME'] == 'GERMANY'], left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Merge partsupp and suppliers on supplier key
partsupp_merged = partsupp_df.merge(suppliers_in_germany, left_on='PS_SUPPKEY', right_on='S_SUPPKEY')

# Calculating the value for each part
partsupp_merged['VALUE'] = partsupp_merged['PS_SUPPLYCOST'] * partsupp_merged['PS_AVAILQTY']

# Aggregating values to calculate the total value for each part
part_value_grouped = partsupp_merged.groupby('PS_PARTKEY').agg({'VALUE': 'sum'}).reset_index()

# Threshold calculation (for simplicity, set to a specific value, e.g., 10000)
# This portion is mentioned in the query brief but not explicitly defined.
# To implement a dynamic threshold, you'd need to define a subquery or calculation
threshold = 10000

# Filtering based on the threshold
filtered_parts = part_value_grouped[part_value_grouped['VALUE'] > threshold]

# Order the results
ordered_parts = filtered_parts.sort_values(by='VALUE', ascending=False)

# Save the results to a CSV file
ordered_parts.to_csv('query_output.csv', index=False)
