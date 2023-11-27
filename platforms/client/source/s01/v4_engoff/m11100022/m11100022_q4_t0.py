import pandas as pd
import direct_redis

# Establishing a connection to the Redis database
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis using direct_redis
orders_df = pd.DataFrame.from_dict(redis_connection.get('orders'))
lineitem_df = pd.DataFrame.from_dict(redis_connection.get('lineitem'))

# Convert date strings to datetime objects for comparison
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Filtering the orders and lineitems based on the required conditions
filtered_orders = orders_df[(orders_df['O_ORDERDATE'] >= pd.Timestamp('1993-07-01')) & (orders_df['O_ORDERDATE'] <= pd.Timestamp('1993-10-01'))]
late_lineitems = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]

# Merging orders with lineitems to check for lineitems that meet the condition per order
late_orders_with_lineitems = pd.merge(filtered_orders, late_lineitems, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Getting the count of such orders for each order priority
output = late_orders_with_lineitems.groupby('O_ORDERPRIORITY').agg({'O_ORDERKEY':'nunique'}).rename(columns={'O_ORDERKEY': 'order_count'}).reset_index()

# Sorting by order priority
output = output.sort_values('O_ORDERPRIORITY', ascending=True)

# Writing the result to a CSV file
output.to_csv('query_output.csv', index=False)
