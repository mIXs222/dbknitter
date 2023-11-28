import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to the Redis database
redis_host = 'redis'
redis_port = 6379
r = DirectRedis(host=redis_host, port=redis_port, db=0)

# Read data from Redis
orders_df = pd.read_json(r.get('orders'))
lineitem_df = pd.read_json(r.get('lineitem'))

# Convert columns to datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Define the time frame
start_date = '1993-07-01'
end_date = '1993-10-01'

# Filter data based on time frame and conditions specified
filtered_orders = orders_df[
    (orders_df['O_ORDERDATE'] >= start_date) & (orders_df['O_ORDERDATE'] <= end_date)
]

filtered_lineitem = lineitem_df[
    lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']
]

# Merge orders and lineitem on L_ORDERKEY and O_ORDERKEY
merged_df = pd.merge(
    filtered_orders,
    filtered_lineitem,
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY'
)

# Count the qualified orders for each order priority
priority_counts = merged_df.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index()
priority_counts.columns = ['Order Priority', 'Order Count']

# Sort by order priority
priority_counts.sort_values(by='Order Priority', inplace=True)

# Write to CSV
priority_counts.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
