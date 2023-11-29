# filename: query.py

import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
hostname = 'redis'
port = 6379
redis_client = DirectRedis(host=hostname, port=port, db=0)

# Retrieve the orders and lineitem table from Redis
orders_df_raw = redis_client.get('orders')
lineitem_df_raw = redis_client.get('lineitem')

# Convert the string representations into Pandas DataFrames
orders_df = pd.read_json(orders_df_raw)
lineitem_df = pd.read_json(lineitem_df_raw)

# Filter orders and lineitem based on the dates and whether lineitem was received later than committed
filtered_orders_df = orders_df[(orders_df['O_ORDERDATE'] >= '1993-07-01') & (orders_df['O_ORDERDATE'] <= '1993-10-01')]
filtered_lineitem_df = lineitem_df[(lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE'])]

# Join Orders and Lineitem data
joined_data = pd.merge(filtered_orders_df, filtered_lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

# Get the orders with received date later than the commit date
late_orders = joined_data.drop_duplicates(subset=['O_ORDERKEY'])

# Count orders by priority
order_count_by_priority = late_orders.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

# Sort the result by order priority
order_count_by_priority_sorted = order_count_by_priority.sort_values(by='O_ORDERPRIORITY')

# Output the result as a CSV file
order_count_by_priority_sorted.to_csv('query_output.csv', index=False)
