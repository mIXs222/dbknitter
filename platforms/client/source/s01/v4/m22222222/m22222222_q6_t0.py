# query.py
import pandas as pd
import direct_redis

# Connection details
hostname = 'redis'
port = 6379
database_name = '0'

# Connect to Redis
connection = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Fetch lineitem data from Redis
data = connection.get('lineitem')
df = pd.read_json(data)

# Filter the dataframe based on the conditions specified in the query
filtered_df = df[
    (df['L_SHIPDATE'] >= '1994-01-01') &
    (df['L_SHIPDATE'] < '1995-01-01') &
    (df['L_DISCOUNT'] >= .06 - 0.01) &
    (df['L_DISCOUNT'] <= .06 + 0.01) &
    (df['L_QUANTITY'] < 24)
]

# Calculate REVENUE
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']

# Compute sum of REVENUE and output to csv
result = filtered_df[['REVENUE']].sum()
result.to_csv('query_output.csv', index=False)
