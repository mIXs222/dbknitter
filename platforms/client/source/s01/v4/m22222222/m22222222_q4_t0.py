import pandas as pd
import direct_redis

# Create a connection to the Redis database
connection_info = {'host': 'redis', 'port': 6379, 'db': 0}
redis_connection = direct_redis.DirectRedis(**connection_info)

# Read the orders and lineitem tables from Redis
orders = pd.DataFrame(redis_connection.get('orders'))
lineitem = pd.DataFrame(redis_connection.get('lineitem'))

# Convert columns to appropriate data types
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
lineitem['L_COMMITDATE'] = pd.to_datetime(lineitem['L_COMMITDATE'])
lineitem['L_RECEIPTDATE'] = pd.to_datetime(lineitem['L_RECEIPTDATE'])

# Filter orders based on O_ORDERDATE
filtered_orders = orders[
    (orders['O_ORDERDATE'] >= '1993-07-01') & 
    (orders['O_ORDERDATE'] < '1993-10-01')
]

# Join orders with lineitem on O_ORDERKEY = L_ORDERKEY
joined_data = filtered_orders.merge(
    lineitem[lineitem['L_COMMITDATE'] < lineitem['L_RECEIPTDATE']],
    left_on='O_ORDERKEY', right_on='L_ORDERKEY',
    how='inner'
)

# Perform GROUP BY and COUNT(*) operation
result = joined_data.groupby('O_ORDERPRIORITY', as_index=False).size()

# Rename the columns
result.rename(columns={'size': 'ORDER_COUNT'}, inplace=True)

# Sort the results by O_ORDERPRIORITY
result = result.sort_values(by='O_ORDERPRIORITY')

# Write output to CSV file
result.to_csv('query_output.csv', index=False)
