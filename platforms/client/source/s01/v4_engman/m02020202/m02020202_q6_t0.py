import pandas as pd
import direct_redis

# Connect to Redis
hostname = 'redis'
port = 6379
database_name = '0'
redis_client = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Retrieve the 'lineitem' table from Redis as a Pandas DataFrame
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter the DataFrame based on the query conditions
filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] > '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
    (lineitem_df['L_DISCOUNT'] >= (0.06 - 0.01)) &
    (lineitem_df['L_DISCOUNT'] <= (0.06 + 0.01)) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']

# Output only the REVENUE column sum
revenue_sum = filtered_df[['REVENUE']].sum()

# Write output to a CSV file
revenue_sum.to_csv('query_output.csv', header=True)
