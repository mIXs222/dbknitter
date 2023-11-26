# python_code.py

import pandas as pd
from direct_redis import DirectRedis

# Establish the connection to Redis
redis_host = "redis"
redis_port = 6379
redis_db = 0
redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Retrieve the data from Redis
orders_data = redis_client.get('orders')
lineitem_data = redis_client.get('lineitem')

# Convert the data from Redis to Pandas DataFrame
orders_df = pd.read_json(orders_data)
lineitem_df = pd.read_json(lineitem_data)

# Convert date strings to datetime objects for comparison
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Filter the orders based on the O_ORDERDATE condition
filtered_orders = orders_df[
    (orders_df['O_ORDERDATE'] >= '1993-07-01') & 
    (orders_df['O_ORDERDATE'] < '1993-10-01')
]

# Filter the lineitem table to find rows with L_COMMITDATE < L_RECEIPTDATE
filtered_lineitem = lineitem_df[
    lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']
]

# Merge filtered_orders and filtered_lineitem on L_ORDERKEY = O_ORDERKEY
merged_data = pd.merge(
    filtered_orders, 
    filtered_lineitem, 
    left_on='O_ORDERKEY', 
    right_on='L_ORDERKEY'
)

# Group data by O_ORDERPRIORITY and count
output_data = merged_data.groupby('O_ORDERPRIORITY').size().reset_index(name='ORDER_COUNT')

# Sort by O_ORDERPRIORITY
output_data = output_data.sort_values(by='O_ORDERPRIORITY')

# Write the result to a CSV file
output_data.to_csv('query_output.csv', index=False)
