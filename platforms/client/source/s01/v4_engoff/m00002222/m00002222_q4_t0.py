import direct_redis
import pandas as pd

# Establish connection to Redis
hostname = 'redis'
port = 6379
db_number = 0
redis_client = direct_redis.DirectRedis(host=hostname, port=port, db=db_number)

# Retrieve data from Redis
orders = pd.read_json(redis_client.get('orders'))
lineitem = pd.read_json(redis_client.get('lineitem'))

# Convert date strings to datetime objects for filtering
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
lineitem['L_RECEIPTDATE'] = pd.to_datetime(lineitem['L_RECEIPTDATE'])
lineitem['L_COMMITDATE'] = pd.to_datetime(lineitem['L_COMMITDATE'])

# Filter orders based on the date range
filtered_orders = orders[(orders['O_ORDERDATE'] >= '1993-07-01') & (orders['O_ORDERDATE'] <= '1993-10-01')]

# Join orders with lineitems on order key
joined_data = pd.merge(filtered_orders, lineitem, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Identify orders where at least one lineitem was received later than its committed date
late_orders = joined_data[joined_data['L_RECEIPTDATE'] > joined_data['L_COMMITDATE']]

# Group by order priority and count unique orders
order_priority_count = late_orders.groupby('O_ORDERPRIORITY')['O_ORDERKEY'].nunique().reset_index(name='order_count')

# Sort the results by order priority
sorted_order_priority_count = order_priority_count.sort_values(by='O_ORDERPRIORITY')

# Write the sorted results to a CSV file
sorted_order_priority_count.to_csv('query_output.csv', index=False)
