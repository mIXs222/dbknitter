import pandas as pd
import direct_redis

# Connection settings
hostname = 'redis'
port = 6379
database_name = '0'

# Connect to Redis
client = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Read orders and lineitem tables from Redis
orders = pd.DataFrame(client.get('orders'))
lineitem = pd.DataFrame(client.get('lineitem'))

# Convert columns to appropriate datatypes
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
lineitem['L_COMMITDATE'] = pd.to_datetime(lineitem['L_COMMITDATE'])
lineitem['L_RECEIPTDATE'] = pd.to_datetime(lineitem['L_RECEIPTDATE'])

# Filter necessary timeframe
orders_timeframe = orders.loc[(orders['O_ORDERDATE'] >= '1993-07-01') & (orders['O_ORDERDATE'] <= '1993-10-01')]

# Select line items with a commitment date before the receipt date
committed_lineitems = lineitem.loc[lineitem['L_COMMITDATE'] < lineitem['L_RECEIPTDATE']]

# Join orders with the subset of line items
filtered_orders = orders_timeframe.merge(committed_lineitems, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Count orders per priority after filtering
order_count_by_priority = filtered_orders.groupby('O_ORDERPRIORITY', as_index=False)['O_ORDERKEY'].nunique()

# Sort results based on order priority
sorted_order_count = order_count_by_priority.sort_values(by='O_ORDERPRIORITY')

# Write to CSV file
sorted_order_count.to_csv('query_output.csv', index=False)
