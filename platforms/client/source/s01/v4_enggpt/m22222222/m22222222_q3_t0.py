# query.py
import pandas as pd
from direct_redis import DirectRedis

# Redis connection and query
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0

# Establish Redis connection
r = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Load data from Redis
customer_df = pd.read_json(r.get('customer'))
orders_df = pd.read_json(r.get('orders'))
lineitem_df = pd.read_json(r.get('lineitem'))

# Convert columns with dates to datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter the dataframes based on the given conditions
customer_build_df = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']
orders_before_date_df = orders_df[orders_df['O_ORDERDATE'] < '1995-03-15']
lineitem_after_date_df = lineitem_df[lineitem_df['L_SHIPDATE'] > '1995-03-15']

# Merge the dataframes to create a combined dataset
merged_df = customer_build_df.merge(orders_before_date_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(lineitem_after_date_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Compute the revenue and assign as a new field
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group by O_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY and calculate total revenue
result_df = merged_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'], as_index=False)['REVENUE'].sum()

# Sort the results by REVENUE descending and O_ORDERDATE ascending
result_df = result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write the results to a CSV file
result_df.to_csv('query_output.csv', index=False)
