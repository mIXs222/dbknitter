# analysis.py
import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis database
redis_host = 'redis'
redis_port = 6379
redis_db = 0
redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Read data from redis
orders_df = pd.DataFrame(redis_client.get('orders'))
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Convert string dates to datetime
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Filter based on dates and create a combined DataFrame
date_mask = lineitem_df['L_RECEIPTDATE'].between('1994-01-01', '1994-12-31')
lineitem_df = lineitem_df[date_mask]

# Merge orders and lineitem tables based on order key
merged_df = pd.merge(
    lineitem_df[lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']],
    orders_df[(orders_df['O_ORDERDATE'] < orders_df['O_ORDERDATE'] + pd.DateOffset(years=1)) 
              & (orders_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH']))],
    left_on='L_ORDERKEY',
    right_on='O_ORDERKEY',
    how='inner'
)

# Apply filter criteria
filter_conditions = (
    (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) &
    (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP']))
)
filtered_df = merged_df[filter_conditions]

# Summarize high and low priority line items for each SHIPMODE
priority_summary = filtered_df.groupby(['L_SHIPMODE', 'O_ORDERPRIORITY']).size().unstack(fill_value=0)
priority_summary.columns = ['HIGH_LINE_COUNT', 'LOW_LINE_COUNT']

# Save results to CSV
priority_summary.reset_index().sort_values('L_SHIPMODE').to_csv('query_output.csv', index=False)
