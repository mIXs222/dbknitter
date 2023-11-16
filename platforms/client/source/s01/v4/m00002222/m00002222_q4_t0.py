import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to the Redis database
r = DirectRedis(host='redis', port=6379, db=0)

# Function to get data from Redis
def get_data_from_redis(key):
    data = r.get(key)
    if data:
        return pd.read_json(data)
    else:
        return pd.DataFrame()

# Get orders and lineitem data from Redis
orders_df = get_data_from_redis('orders')
lineitem_df = get_data_from_redis('lineitem')

# Convert date strings to datetime objects for comparison
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Filter orders by date range
filtered_orders = orders_df[
    (orders_df['O_ORDERDATE'] >= '1993-07-01') &
    (orders_df['O_ORDERDATE'] < '1993-10-01')
]

# Check if lineitem conditions are met
lineitem_condition = lineitem_df[
    lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']
]

# Inner join filtered orders with lineitem_condition on L_ORDERKEY
result = pd.merge(
    filtered_orders,
    lineitem_condition,
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY',
    how='inner'
)

# Perform GROUP BY
grouped_result = result.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

# Sort the result
sorted_grouped_result = grouped_result.sort_values('O_ORDERPRIORITY')

# Write the result to query_output.csv
sorted_grouped_result.to_csv('query_output.csv', index=False)
