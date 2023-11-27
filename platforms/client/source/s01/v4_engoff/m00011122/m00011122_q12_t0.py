import pandas as pd
from direct_redis import DirectRedis

# Initialize connection
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Fetch the data from Redis
orders = pd.read_json(redis_connection.get('orders'))
lineitem = pd.read_json(redis_connection.get('lineitem'))

# Merge orders and lineitem tables on the order key
merged_df = pd.merge(orders, lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter data according to specified conditions and select columns
filtered_df = merged_df[(
    (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (merged_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH'])) &
    (merged_df['L_RECEIPTDATE'] >= '1994-01-01') &
    (merged_df['L_RECEIPTDATE'] <= '1995-01-01') &
    (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) &
    (merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE'])
)]

# Group by ship mode and order priority, then count
output_df = filtered_df.groupby(['L_SHIPMODE', 'O_ORDERPRIORITY']).size().reset_index(name='COUNT')

# Write result to CSV
output_df.to_csv('query_output.csv', index=False)
