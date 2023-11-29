import pandas as pd
from direct_redis import DirectRedis

# Connection Information
hostname = 'redis'
port = 6379
database_name = '0'

# Connect to the Redis database
redis_client = DirectRedis(host=hostname, port=port, db=database_name)

# Get orders and lineitem tables as Pandas DataFrames
orders = pd.read_json(redis_client.get('orders'), orient='records')
lineitem = pd.read_json(redis_client.get('lineitem'), orient='records')

# Convert date strings to pandas datetime format
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
lineitem['L_COMMITDATE'] = pd.to_datetime(lineitem['L_COMMITDATE'])
lineitem['L_RECEIPTDATE'] = pd.to_datetime(lineitem['L_RECEIPTDATE'])

# Filter orders between the dates
filtered_orders = orders[
    (orders['O_ORDERDATE'] >= '1993-07-01') &
    (orders['O_ORDERDATE'] <= '1993-10-01')
]

# Merge orders and line items on the key
merged_data = pd.merge(filtered_orders, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Identify orders with at least one line item received after its committed date
late_orders = merged_data[merged_data['L_RECEIPTDATE'] > merged_data['L_COMMITDATE']]

# Count the number of such orders grouped by O_ORDERPRIORITY
order_counts = late_orders.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index()

# Rename columns as specified
order_counts.columns = ['O_ORDERPRIORITY', 'ORDER_COUNT']

# Sort by order priority
sorted_order_counts = order_counts.sort_values('O_ORDERPRIORITY')

# Write to CSV
sorted_order_counts.to_csv('query_output.csv', index=False)
