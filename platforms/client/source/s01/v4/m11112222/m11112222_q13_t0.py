import pandas as pd
import direct_redis

# Connection information
hostname = 'redis'
port = 6379
database_name = '0'  # Redis doesn't actually use database names, but has numbered databases starting from 0 

# Connect to Redis
r = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Get data from Redis as strings and convert it to Pandas DataFrames
customer_data = r.get('customer')
orders_data = r.get('orders')

# Convert the string data to Pandas DataFrames
customer_df = pd.read_json(customer_data)
orders_df = pd.read_json(orders_data)

# Perform the LEFT OUTER JOIN operation
merged_df = pd.merge(customer_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Filter out the orders with comment NOT LIKE '%pending%deposits%'
filtered_orders_df = merged_df[~merged_df['O_COMMENT'].str.contains("pending%deposits%", na=False, regex=False)]

# Group by customer key and count orders
customer_order_count = filtered_orders_df.groupby('C_CUSTKEY', as_index=False)['O_ORDERKEY'].count().rename(columns={'O_ORDERKEY': 'C_COUNT'})

# Group by count and count distinct customers
output_df = customer_order_count.groupby('C_COUNT', as_index=False).size().rename(columns={'size': 'CUSTDIST'})

# Sort the results as specified
output_df.sort_values(by=['CUSTDIST', 'C_COUNT'], ascending=[False, False], inplace=True)

# Write the output dataframe to csv file
output_df.to_csv('query_output.csv', index=False)
