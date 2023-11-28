import pandas as pd
from direct_redis import DirectRedis

# Establish connection to the Redis database
redis = DirectRedis(host='redis', port=6379, db=0)

# Read the 'orders' and 'lineitem' tables from Redis
orders_df = pd.read_json(redis.get('orders'))
lineitem_df = pd.read_json(redis.get('lineitem'))

# Merge the two dataframes on their order keys
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter based on the specified shipping modes and date conditions
filtered_df = merged_df[
    (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (merged_df['O_ORDERDATE'] >= '1994-01-01') &
    (merged_df['O_ORDERDATE'] <= '1994-12-31') &
    (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) &
    (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE'])
]

# Define high-priority and low-priority conditions
priority_mapping = {'1-URGENT': 'HIGH', '2-HIGH': 'HIGH'}
filtered_df['PRIORITY_TYPE'] = filtered_df['O_ORDERPRIORITY'].map(priority_mapping).fillna('LOW')

# Group by 'L_SHIPMODE' and 'PRIORITY_TYPE' and get counts
summary_df = filtered_df.groupby(['L_SHIPMODE', 'PRIORITY_TYPE']).size().unstack(fill_value=0).reset_index()
summary_df.columns = ['SHIPPING_MODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT']

# Sort the results by shipping mode
summary_df.sort_values(by='SHIPPING_MODE', inplace=True)

# Write the results to a CSV file
summary_df.to_csv('query_output.csv', index=False)
