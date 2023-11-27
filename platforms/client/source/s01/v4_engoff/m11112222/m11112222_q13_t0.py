import pandas as pd
import direct_redis

# Connect to Redis
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0
dr = direct_redis.DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Load data from Redis
try:
    customer_df = pd.read_json(dr.get('customer'))
    orders_df = pd.read_json(dr.get('orders'))
except Exception as e:
    print(f"Error loading data from Redis: {e}")
    exit()

# Process the data
# Ensuring the orders status is not 'pending' or 'deposits' with exclusion in order comments.
orders_df_filtered = orders_df[~orders_df['O_ORDERSTATUS'].str.contains('pending|deposits', case=False, regex=True)]

# Count the number of orders per customer
orders_count = orders_df_filtered.groupby('O_CUSTKEY').size().reset_index(name='order_count')

# Combine with customer table and fill customers with no orders as 0
customer_order_distribution = customer_df.merge(orders_count, how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY')
customer_order_distribution['order_count'] = customer_order_distribution['order_count'].fillna(0)

# Count the number of customers by the number of orders
customer_distribution = customer_order_distribution.groupby('order_count').size().reset_index(name='customer_count')

# Write the results to a CSV
customer_distribution.to_csv('query_output.csv', index=False)
