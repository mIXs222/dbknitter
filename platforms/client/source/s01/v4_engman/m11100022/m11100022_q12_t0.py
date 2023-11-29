import pandas as pd
import direct_redis

# Connect to the Redis database
hostname = 'redis'
port = 6379
dr = direct_redis.DirectRedis(host=hostname, port=port, db=0)

# Read the data from Redis
orders_df = pd.read_json(dr.get('orders'))
lineitem_df = pd.read_json(dr.get('lineitem'))

# Perform the query as per the requirements
filtered_lineitems = lineitem_df[
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) & 
    (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']) & 
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) & 
    (lineitem_df['L_RECEIPTDATE'] >= '1994-01-01') & 
    (lineitem_df['L_RECEIPTDATE'] <= '1995-01-01')
]

# Join to get the order priority
result = pd.merge(
    filtered_lineitems,
    orders_df[['O_ORDERKEY', 'O_ORDERPRIORITY']],
    left_on='L_ORDERKEY',
    right_on='O_ORDERKEY',
    how='inner'
)

# Create the output dataframe
output_df = result.groupby('L_SHIPMODE').apply(
    lambda df: pd.Series({
        'Count_High_Priority': df[df['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH'])].shape[0],
        'Count_Low_Priority': df[~df['O_ORDERPRIORITY'].isin(['URGENT', 'HIGH'])].shape[0]
    })
).reset_index().sort_values(by='L_SHIPMODE')

# Write the result to a CSV file
output_df.to_csv('query_output.csv', index=False)
