import pandas as pd
from direct_redis import DirectRedis

# Connect to redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Read data from Redis into Pandas DataFrames
df_nation = pd.read_json(redis_connection.get('nation'))
df_supplier = pd.read_json(redis_connection.get('supplier'))
df_partsupp = pd.read_json(redis_connection.get('partsupp'))

# Perform SQL-like join and query operations in Pandas
merged_df = df_partsupp.merge(df_supplier, left_on='PS_SUPPKEY', right_on='S_SUPPKEY').merge(df_nation, left_on='S_NATIONKEY', right_on='N_NATIONKEY')
filtered_df = merged_df[merged_df['N_NAME'] == 'GERMANY']

# Compute the sum of PS_SUPPLYCOST * PS_AVAILQTY per PS_PARTKEY
grouped = filtered_df.groupby('PS_PARTKEY').apply(lambda x: (x['PS_SUPPLYCOST'] * x['PS_AVAILQTY']).sum()).reset_index(name='VALUE')

# Compute the global sum of PS_SUPPLYCOST * PS_AVAILQTY
global_sum = (filtered_df['PS_SUPPLYCOST'] * filtered_df['PS_AVAILQTY']).sum() * 0.0001000000

# Filter groups that have a sum greater than the global sum
result_df = grouped[grouped['VALUE'] > global_sum]

# Sort the resulting DataFrame
result_df_sorted = result_df.sort_values(by='VALUE', ascending=False)

# Write results to CSV file
result_df_sorted.to_csv('query_output.csv', index=False)
