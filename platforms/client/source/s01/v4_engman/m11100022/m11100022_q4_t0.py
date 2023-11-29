import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to Redis
redis_host = "redis"
redis_port = 6379
redis_db = 0
r = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Load the data from Redis
try:
    orders_df = pd.read_json(r.get('orders'))
    lineitem_df = pd.read_json(r.get('lineitem'))
except Exception as e:
    print(f"An error occurred when reading data from Redis: {e}")
    raise

# Filter the data based on the date criteria
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
filtered_orders = orders_df[(orders_df['O_ORDERDATE'] >= '1993-07-01') & (orders_df['O_ORDERDATE'] <= '1993-10-01')]

# Filter lineitem by L_COMMITDATE and L_RECEIPTDATE
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
late_lineitems = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]

# Join the dataframes on the order key
orders_with_late_lineitems = filtered_orders.merge(late_lineitems, left_on='O_ORDERKEY', right_on='L_ORDERKEY', how='inner')

# Count the number of orders for each priority
order_counts = orders_with_late_lineitems.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index(name='ORDER_COUNT')

# Sort by priority ascending
sorted_order_counts = order_counts.sort_values(by='O_ORDERPRIORITY')

# Output the columns as ORDER_COUNT then O_ORDERPRIORITY
final_output = sorted_order_counts[['ORDER_COUNT', 'O_ORDERPRIORITY']]

# Write to CSV
final_output.to_csv('query_output.csv', index=False)
