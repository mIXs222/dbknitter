import pandas as pd
from direct_redis import DirectRedis

# Establish connection to Redis
redis_host = 'redis'
redis_port = 6379
redis_db = 0
redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Retrieve data from Redis
orders_df = pd.read_json(redis_client.get('orders'), orient='index')
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='index')

# Merge DataFrames on their keys
merged_df = pd.merge(
    orders_df,
    lineitem_df,
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY',
    how='inner'
)

# Perform the analysis
high_priority_modes = ['MAIL', 'SHIP']
filtered_df = merged_df[
    (merged_df['L_SHIPMODE'].isin(high_priority_modes)) &
    (merged_df['L_RECEIPTDATE'] >= '1994-01-01') &
    (merged_df['L_RECEIPTDATE'] <= '1995-01-01') &
    (merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE']) &
    (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE'])
]

# Classify order priority
filtered_df['PRIORITY_GROUP'] = filtered_df['O_ORDERPRIORITY'].apply(
    lambda x: 'HIGH' if x in ['URGENT', 'HIGH'] else 'LOW'
)

# Group and aggregate data
result = filtered_df.groupby('L_SHIPMODE')['PRIORITY_GROUP'].value_counts().unstack(fill_value=0)
result.reset_index(inplace=True)
result.sort_values(by='L_SHIPMODE', ascending=True, inplace=True)

# Write output to CSV
result.to_csv('query_output.csv', index=False)
