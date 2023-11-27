import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# connect to redis server
r = DirectRedis(host='redis', port=6379, db=0)

# get DataFrames from redis
orders_df = r.get('orders')
lineitem_df = r.get('lineitem')

# convert to datetime format
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# filter dates
filtered_orders_df = orders_df[(orders_df['O_ORDERDATE'] >= '1993-07-01') &
(orders_df['O_ORDERDATE'] < '1993-10-01')]

# join
merged_df = pd.merge(filtered_orders_df, lineitem_df,
on='O_ORDERKEY')

# perform EXISTS statement
merged_df = merged_df[merged_df['L_COMMITDATE'] <
merged_df['L_RECEIPTDATE']]

# perform group by
result = merged_df['O_ORDERPRIORITY'].value_counts()

# convert series to dataframe
result_df = result.to_frame().reset_index()

# rename columns
result_df.columns = ['O_ORDERPRIORITY', 'ORDER_COUNT']

# sort dataframe
result_df.sort_values(by='O_ORDERPRIORITY', inplace=True)

# write to .csv
result_df.to_csv('query_output.csv', index=False)
