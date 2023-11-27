import pandas as pd
from direct_redis import DirectRedis

# Create a connection to Redis
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0
redis_client = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Retrieve data from Redis
customer_data = pd.read_json(redis_client.get('customer'), orient='records')
orders_data = pd.read_json(redis_client.get('orders'), orient='records')

# Perform the analysis
# Filter out orders that have pending status or contain 'pending' or 'deposits' in the comment
orders_filtered = orders_data[
    ~orders_data['O_ORDERSTATUS'].isin(['pending', 'deposits']) &
    ~orders_data['O_COMMENT'].str.contains('pending|deposits', case=False, na=False)
]

# Merge the customer data with the filtered orders data
customer_orders = customer_data.merge(
    orders_filtered.groupby('O_CUSTKEY').size().reset_index(name='order_count'),
    how='left', left_on='C_CUSTKEY', right_on='O_CUSTKEY'
)

# Fill NaN values in the order_count column with 0 to indicate no orders for certain customers
customer_orders['order_count'].fillna(0, inplace=True)

# Calculate the distribution of customers by the number of orders
customer_distribution = customer_orders.groupby('order_count').size().reset_index(name='customer_count')

# Write the result to a CSV file
customer_distribution.to_csv('query_output.csv', index=False)
