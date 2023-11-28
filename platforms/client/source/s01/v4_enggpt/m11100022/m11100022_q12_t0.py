# analysis.py

import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Establish a connection to Redis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Read tables from Redis as Pandas DataFrames
orders_df = pd.read_json(redis_connection.get('orders'))
lineitem_df = pd.read_json(redis_connection.get('lineitem'))

# Convert string dates to datetime format
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Define date range
start_date = datetime(1994, 1, 1)
end_date = datetime(1994, 12, 31)

# Filter lineitem table based on dates and shipping modes
filtered_lineitem = lineitem_df[
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']) &
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_RECEIPTDATE'] >= start_date) &
    (lineitem_df['L_RECEIPTDATE'] <= end_date)
]

# Merge orders and lineitem DataFrames
merged_df = pd.merge(filtered_lineitem, orders_df, how='inner', left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Define priority groups
high_priority = ['1-URGENT', '2-HIGH']
low_priority = ['3-MEDIUM', '4-NOT SPECIFIED', '5-LOW']

# Create a priority column based on the O_ORDERPRIORITY
merged_df['PRIORITY'] = merged_df['O_ORDERPRIORITY'].apply(lambda x: 'HIGH' if x in high_priority else 'LOW')

# Group by shipping mode and priority
grouped = merged_df.groupby(['L_SHIPMODE', 'PRIORITY'])

# Count line items for each group
result_df = grouped.size().unstack(fill_value=0).rename(columns={'HIGH': 'HIGH_LINE_COUNT', 'LOW': 'LOW_LINE_COUNT'})

# Sort by shipping mode
result_df = result_df.sort_index()

# Write results to CSV
result_df.to_csv('query_output.csv')
