import pandas as pd
import direct_redis

# Connection info
hostname = 'redis'
port = 6379
database_name = '0'

# Establish the connection to Redis
redis_client = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Read data from Redis
orders_df = pd.DataFrame(redis_client.get('orders'))
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Convert data to proper types
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filtering criteria
time_filter = (lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']) & \
              (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) & \
              (lineitem_df['L_RECEIPTDATE'] >= '1994-01-01') & \
              (lineitem_df['L_RECEIPTDATE'] <= '1994-12-31')

# Ship modes of interest
ship_modes = ['MAIL', 'SHIP']

# Apply the filtering criteria
filtered_lineitems = lineitem_df[time_filter & lineitem_df['L_SHIPMODE'].isin(ship_modes)]

# Merge with orders on the order key
merged_df = filtered_lineitems.merge(orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# High priority and Low priority flags
merged_df['HIGH_PRIORITY'] = merged_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])

# Group the data
grouped = merged_df.groupby(['L_SHIPMODE', 'HIGH_PRIORITY'])

# Count the line items
line_count = grouped['L_ORDERKEY'].count().unstack(fill_value=0).rename(columns={True: 'HIGH_LINE_COUNT', False: 'LOW_LINE_COUNT'})

# Save the results to a CSV file
line_count.sort_values(by='L_SHIPMODE').to_csv('query_output.csv')
