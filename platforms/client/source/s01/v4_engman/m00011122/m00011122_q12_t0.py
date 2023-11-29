import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
redis_host = "redis"
redis_port = 6379
redis_db = 0
redis_conn = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Retrieve data from Redis as pandas DataFrames
orders_df = pd.read_json(redis_conn.get('orders'))
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Merge the tables on the order key fields
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter records based on the given conditions
filtered_df = merged_df[
    (merged_df['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH'])) &
    (merged_df['L_RECEIPTDATE'] >= '1994-01-01') &
    (merged_df['L_RECEIPTDATE'] < '1995-01-01') &
    (merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE']) &
    (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) &
    (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP']))
]

# Group by ship mode and count line items of high order priority or not
results = filtered_df.groupby('L_SHIPMODE').agg(
    high_priority_count=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: (x == 'URGENT').sum()),
    low_priority_count=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: (x != 'URGENT').sum())
).reset_index()

# Write results to a CSV file
results.to_csv('query_output.csv', index=False)
