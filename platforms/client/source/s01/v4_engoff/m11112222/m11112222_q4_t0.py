import pandas as pd
import direct_redis
import csv

# Connection to the Redis database
connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis
orders_df = pd.read_json(connection.get('orders'))
lineitem_df = pd.read_json(connection.get('lineitem'))

# Merge DataFrames on the shared key
merged_df = orders_df.merge(lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter by dates and check if the receive date is later than the commit date
filtered_df = merged_df[
    (merged_df['O_ORDERDATE'] >= '1993-07-01') & 
    (merged_df['O_ORDERDATE'] <= '1993-10-01') & 
    (merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE'])
]

# Count the number of such orders for each order priority
result_df = filtered_df.groupby('O_ORDERPRIORITY').size().reset_index(name='order_count')

# Sort the result by order priority
sorted_result_df = result_df.sort_values(by='O_ORDERPRIORITY')

# Write the output to the CSV file
sorted_result_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
