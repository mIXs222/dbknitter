import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis database
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Read tables from Redis into Pandas DataFrames
orders_df = pd.read_json(redis_connection.get('orders'))
lineitem_df = pd.read_json(redis_connection.get('lineitem'))

# Convert string dates to datetime objects
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Filter orders within the required date range
filtered_orders = orders_df[
    (orders_df['O_ORDERDATE'] >= '1993-07-01') &
    (orders_df['O_ORDERDATE'] < '1993-10-01')
]

# Merge filtered orders with lineitem on order key
merged_df = pd.merge(
    filtered_orders,
    lineitem_df,
    how='inner',
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY'
)

# Find orders with at least one lineitem received later than committed date
late_orders = merged_df[
    merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE']
]

# Count such orders for each order priority
priority_counts = late_orders.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index()

# Sort by priority (assuming the priorities are categorical with a known order)
# If the priorities are labeled numerically or alphabetically, we can sort directly.
# Here I'll assume they are labeled like "PRIORITY 1", "PRIORITY 2", etc.
priority_counts['O_ORDERPRIORITY'] = pd.Categorical(priority_counts['O_ORDERPRIORITY'],
                                                     categories=[f'PRIORITY {i}' for i in range(1, 6)],
                                                     ordered=True)
priority_counts_sorted = priority_counts.sort_values('O_ORDERPRIORITY')

# Write result to query_output.csv
priority_counts_sorted.to_csv('query_output.csv', index=False)
