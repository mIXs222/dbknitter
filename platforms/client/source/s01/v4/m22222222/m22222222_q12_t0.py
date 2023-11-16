import pandas as pd
import direct_redis

# Connect to Redis
hostname = 'redis'
port = 6379
database_name = '0'
r = direct_redis.DirectRedis(host=hostname, port=port, db=database_name)

# Read data from Redis
orders_df = pd.DataFrame(eval(r.get('orders')))
lineitem_df = pd.DataFrame(eval(r.get('lineitem')))

# Merge data on the key
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Apply filters
filtered_df = merged_df[
    (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) &
    (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) &
    (merged_df['L_RECEIPTDATE'] >= '1994-01-01') &
    (merged_df['L_RECEIPTDATE'] < '1995-01-01')
]

# Calculate aggregates
aggregated_df = filtered_df.groupby('L_SHIPMODE').agg(
    HIGH_LINE_COUNT=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: sum((x == '1-URGENT') | (x == '2-HIGH'))),
    LOW_LINE_COUNT=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: sum((x != '1-URGENT') & (x != '2-HIGH')))
).reset_index()

# Sort by SHIP MODE
aggregated_df.sort_values('L_SHIPMODE', inplace=True)

# Output the results to a CSV file
aggregated_df.to_csv('query_output.csv', index=False)
