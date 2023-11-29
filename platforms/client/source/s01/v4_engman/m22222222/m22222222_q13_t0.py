from direct_redis import DirectRedis
import pandas as pd

# Establish connection
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0
redis_conn = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Read customers and orders table from Redis
customers_df = pd.read_json(redis_conn.get('customer'), orient='records')
orders_df = pd.read_json(redis_conn.get('orders'), orient='records')

# Filter out orders that have the 'pending' or 'deposits' in the comment
filtered_orders_df = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', regex=True)]

# Group by customer and count orders
orders_count_per_customer = filtered_orders_df.groupby('O_CUSTKEY').size().reset_index(name='orders_count')

# Merge with customers
customer_orders = customers_df.merge(orders_count_per_customer, left_on='C_CUSTKEY', right_on='O_CUSTKEY', how='left')

# Replace NaN with 0 for customers without any orders
customer_orders['orders_count'] = customer_orders['orders_count'].fillna(0)

# Group by the number of orders to get the distribution of customers
distribution_of_customers = customer_orders.groupby('orders_count').size().reset_index(name='number_of_customers')

# Write the result to CSV
distribution_of_customers.to_csv('query_output.csv', index=False)
