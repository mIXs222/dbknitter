import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch the data from Redis
orders_df = pd.DataFrame(redis_client.get('orders'))
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Convert columns to appropriate data types
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Join the orders and lineitem tables on the order key
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter based on the conditions provided in the pseudo-query
filtered_df = merged_df[
    (merged_df['L_RECEIPTDATE'] >= pd.Timestamp('1994-01-01')) &
    (merged_df['L_RECEIPTDATE'] <= pd.Timestamp('1995-01-01')) &
    (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE']) &
    (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE'])
]

# Create a flag to identify high and low priority orders
filtered_df['PRIORITY'] = filtered_df['O_ORDERPRIORITY'].apply(lambda x: 'HIGH' if x in ['URGENT', 'HIGH'] else 'LOW')

# Group by the ship mode and calculate the desired counts
result_df = filtered_df.groupby('L_SHIPMODE')['PRIORITY'].value_counts().unstack().fillna(0)
result_df.columns = ['LOW_PRIORITY_COUNT', 'HIGH_PRIORITY_COUNT']

# Sort the result by the ship mode
result_df = result_df.sort_index()

# Write the result to the output CSV file
result_df.to_csv('query_output.csv')
