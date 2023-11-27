import pandas as pd
import direct_redis

# Connect to Redis
redis_host = 'redis'
redis_port = 6379
client = direct_redis.DirectRedis(host=redis_host, port=redis_port, db=0)

# Fetch DataFrames from Redis
df_customer = pd.read_json(client.get('customer'))
df_orders = pd.read_json(client.get('orders'))
df_lineitem = pd.read_json(client.get('lineitem'))

# Perform the Join and filtering operations as per the SQL query
merged_df = df_customer.merge(df_orders, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
merged_df = merged_df.merge(df_lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

filtered_df = merged_df[
    (merged_df['C_MKTSEGMENT'] == 'BUILDING') &
    (merged_df['O_ORDERDATE'] < '1995-03-15') &
    (merged_df['L_SHIPDATE'] > '1995-03-15')
]

# Calculate REVENUE
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * (1 - filtered_df['L_DISCOUNT'])

# Group by necessary columns, perform SORT and SELECT operations
result_df = filtered_df.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'], as_index=False).agg({'REVENUE': 'sum'})

# Sort the result
result_df.sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True], inplace=True)

# Select only the necessary columns
result_df = result_df[['L_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]

# Write the output to a CSV file named 'query_output.csv'
result_df.to_csv('query_output.csv', index=False)
