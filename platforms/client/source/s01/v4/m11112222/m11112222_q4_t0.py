import pandas as pd
from direct_redis import DirectRedis

# Initialize connection to Redis
redis_host = "redis"
redis_port = 6379
redis_db = 0
redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Fetching orders and lineitem DataFrames from Redis
orders_df = pd.read_msgpack(redis_client.get('orders'))
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))

# Filter orders by O_ORDERDATE
filtered_orders = orders_df[
    (orders_df['O_ORDERDATE'] >= '1993-07-01') &
    (orders_df['O_ORDERDATE'] < '1993-10-01')
]

# Join with lineitem to filter further based on L_COMMITDATE and L_RECEIPTDATE
joined_df = filtered_orders.merge(
    lineitem_df[lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']],
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY',
    how='inner'
).groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

# Sort the results
sorted_df = joined_df.sort_values(by='O_ORDERPRIORITY')

# Write results to a CSV
sorted_df.to_csv('query_output.csv', index=False)
