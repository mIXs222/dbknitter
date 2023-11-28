import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Prepare to read data from Redis tables
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Data cleansing and preparation
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filtering data
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']) &
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_RECEIPTDATE'] >= pd.Timestamp('1994-01-01')) &
    (lineitem_df['L_RECEIPTDATE'] <= pd.Timestamp('1994-12-31'))
]

# Merging orders and line items
merged_df = pd.merge(orders_df, filtered_lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Assigning priority
merged_df['PRIORITY'] = merged_df['O_ORDERPRIORITY'].apply(lambda x: 'HIGH' if x in ['1-URGENT', '2-HIGH'] else 'LOW')

# Grouping and counting
result = merged_df.groupby(['L_SHIPMODE', 'PRIORITY']).size().unstack(fill_value=0)

# Renaming columns
result = result.rename(columns={'HIGH': 'HIGH_LINE_COUNT', 'LOW': 'LOW_LINE_COUNT'})

# Reset index to include 'L_SHIPMODE' in the csv
result = result.reset_index()

# Sorting results
result = result.sort_values('L_SHIPMODE')

# Writing to CSV
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
