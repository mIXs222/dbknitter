import pandas as pd
import direct_redis

# Define connection configuration for Redis
config = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Instantiate a DirectRedis object
redis_client = direct_redis.DirectRedis(**config)

# Retrieve data from Redis
customer_df = pd.read_json(redis_client.get('customer'), orient='records')
orders_df = pd.read_json(redis_client.get('orders'), orient='records')
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Convert dates to datetime format for comparison
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter datasets based on given criteria
customer_build_seg = customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']
orders_before_date = orders_df[orders_df['O_ORDERDATE'] < '1995-03-15']
lineitem_after_date = lineitem_df[lineitem_df['L_SHIPDATE'] > '1995-03-15']

# Merge datasets
merged_df = customer_build_seg.merge(orders_before_date, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(lineitem_after_date, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Calculate revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group by required fields and sum up revenue
grouped_df = merged_df.groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'], as_index=False)['REVENUE'].sum()

# Order results based on revenue (descending) and order date (ascending)
result_df = grouped_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True])

# Write the results to a CSV file
result_df.to_csv('query_output.csv', index=False)
