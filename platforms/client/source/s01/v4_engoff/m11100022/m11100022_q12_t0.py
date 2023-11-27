import pandas as pd
from datetime import datetime
import direct_redis

# Establish connection to Redis database
redis_hostname = 'redis'
redis_port = 6379
redis_db = '0'
redis_connection = direct_redis.DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Fetch tables from Redis
orders_df = pd.read_json(redis_connection.get('orders'), orient='records')
lineitem_df = pd.read_json(redis_connection.get('lineitem'), orient='records')

# Merge datasets on common key
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Define date range
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)

# Filter data based on conditions
filtered_df = merged_df[
    (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (merged_df['L_RECEIPTDATE'] >= start_date) &
    (merged_df['L_RECEIPTDATE'] <= end_date) &
    (merged_df['L_COMMITDATE'] < merged_df['L_SHIPDATE']) &
    (merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE'])
]

# Define priority levels
high_priority = ['URGENT', 'HIGH']
filtered_df['PRIORITY_LEVEL'] = filtered_df['O_ORDERPRIORITY'].apply(lambda x: 'HIGH_OR_URGENT' if x in high_priority else 'OTHER')

# Grouping by ship mode and priority level, counting the instances
result = filtered_df.groupby(['L_SHIPMODE', 'PRIORITY_LEVEL']).size().reset_index(name='COUNT')

# Output results to a CSV file
result.to_csv('query_output.csv', index=False)
