# file: query_executor.py

import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to Redis
redis_connection_info = {
    'db': 0,
    'port': 6379,
    'host': 'redis'
}
redis_client = DirectRedis(**redis_connection_info)

# Read data from Redis
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter data
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_RECEIPTDATE'] >= datetime.datetime(1994, 1, 1)) &
    (lineitem_df['L_RECEIPTDATE'] <= datetime.datetime(1995, 1, 1)) &
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP']))
]

# Merge data
merged_df = pd.merge(orders_df, filtered_lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group and count
grouped_df = merged_df.groupby('L_SHIPMODE').agg(
    high_order_priority=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: (x == 'URGENT').sum() + (x == 'HIGH').sum()),
    low_order_priority=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: ((x != 'URGENT') & (x != 'HIGH')).sum())
).reset_index()

# Sort the results
final_results = grouped_df.sort_values('L_SHIPMODE', ascending=True)

# Write to CSV
final_results.to_csv('query_output.csv', index=False)
