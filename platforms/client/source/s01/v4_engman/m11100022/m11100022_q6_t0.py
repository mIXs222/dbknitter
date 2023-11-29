# query.py
import pandas as pd
from direct_redis import DirectRedis

# Redis connection information
redis_hostname = 'redis'
redis_port = 6379
database_name = '0'

# Connect to Redis
redis_client = DirectRedis(host=redis_hostname, port=redis_port, db=database_name)

# Fetch the lineitem data from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter for the required date and discount range, and quantity
filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] > '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
    (lineitem_df['L_DISCOUNT'] >= 0.06 - 0.01) &
    (lineitem_df['L_DISCOUNT'] <= 0.06 + 0.01) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']

# Output the sum of revenue
revenue_sum = filtered_df[['REVENUE']].sum()
revenue_sum.to_csv('query_output.csv', header=True)
