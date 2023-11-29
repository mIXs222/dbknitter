import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
redis_host = 'redis'
redis_port = 6379
redis_db = 0
client = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Load data from Redis into Pandas DataFrame
customer_df = pd.read_json(client.get('customer').decode('utf-8'))
orders_df = pd.read_json(client.get('orders').decode('utf-8'))
lineitem_df = pd.read_json(client.get('lineitem').decode('utf-8'))

# Calculate the total quantity per order by aggregating line items
total_quantity_per_order = lineitem_df.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()

# Merge orders with total_quantity_per_order to filter large orders (> 300)
large_orders = orders_df.merge(total_quantity_per_order, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
large_orders = large_orders[large_orders['L_QUANTITY'] > 300]

# Select the required fields from customers and large orders
result = customer_df[['C_CUSTKEY', 'C_NAME']].merge(
    large_orders[['O_ORDERKEY', 'O_CUSTKEY', 'O_TOTALPRICE', 'O_ORDERDATE', 'L_QUANTITY']],
    left_on='C_CUSTKEY',
    right_on='O_CUSTKEY',
    how='inner'
)

# Sort the result
result = result.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Select the required columns and rename them
result = result[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Write the output to a CSV file
result.to_csv('query_output.csv', index=False)
