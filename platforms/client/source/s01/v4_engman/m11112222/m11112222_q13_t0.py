# query_code.py

import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to the Redis database
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0
r = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Retrieve the data from Redis tables
customer_df = pd.read_json(r.get('customer'))
orders_df = pd.read_json(r.get('orders'))

# Filter out orders with comments that are like '%pending%deposits%'
filtered_orders = orders_df[~orders_df['O_COMMENT'].str.contains('pending|deposits', regex=True)]

# Group by customer key and count the orders per customer
order_counts = filtered_orders.groupby('O_CUSTKEY').size().reset_index(name='number_of_orders')

# Count how many customers made each number of orders
customers_count = order_counts.groupby('number_of_orders').size().reset_index(name='number_of_customers')

# Save the result to CSV
customers_count.to_csv('query_output.csv', index=False)
