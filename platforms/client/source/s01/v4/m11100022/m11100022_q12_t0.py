import pandas as pd
from direct_redis import DirectRedis
import csv

# Establish connection to the redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch the data from Redis
orders_data = redis_client.get('orders')
lineitem_data = redis_client.get('lineitem')

# Convert data to Pandas DataFrame
orders_df = pd.read_json(orders_data)
lineitem_df = pd.read_json(lineitem_data)

# Perform the join, filtering, and grouping operation
merged_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
filtered_df = merged_df[
    (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) &
    (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) &
    (merged_df['L_RECEIPTDATE'] >= '1994-01-01') &
    (merged_df['L_RECEIPTDATE'] < '1995-01-01')
]

# Aggregating the results
result_df = filtered_df.groupby('L_SHIPMODE').apply(
    lambda df: pd.Series({
        'HIGH_LINE_COUNT': df[(df['O_ORDERPRIORITY'] == '1-URGENT') | (df['O_ORDERPRIORITY'] == '2-HIGH')].shape[0],
        'LOW_LINE_COUNT': df[(df['O_ORDERPRIORITY'] != '1-URGENT') & (df['O_ORDERPRIORITY'] != '2-HIGH')].shape[0]
    })
).reset_index()

# Sorting the results
result_df = result_df.sort_values('L_SHIPMODE')

# Write to a CSV file
result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
