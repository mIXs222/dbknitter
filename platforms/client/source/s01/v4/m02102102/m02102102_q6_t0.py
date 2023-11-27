import pandas as pd
import direct_redis

# Connection details for Redis
hostname = 'redis'
port = 6379
database = '0'

# Connect to Redis
connection = direct_redis.DirectRedis(host=hostname, port=port, db=database)

# Retrieve the data as a Pandas DataFrame
lineitem_data = connection.get('lineitem')
lineitem_df = pd.read_msgpack(lineitem_data)

# Convert columns to proper types
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_EXTENDEDPRICE'] = lineitem_df['L_EXTENDEDPRICE'].astype(float)
lineitem_df['L_DISCOUNT'] = lineitem_df['L_DISCOUNT'].astype(float)
lineitem_df['L_QUANTITY'] = lineitem_df['L_QUANTITY'].astype(int)

# Apply the SQL logic in Pandas
filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= pd.Timestamp('1994-01-01')) &
    (lineitem_df['L_SHIPDATE'] < pd.Timestamp('1995-01-01')) &
    (lineitem_df['L_DISCOUNT'] >= 0.06 - 0.01) &
    (lineitem_df['L_DISCOUNT'] <= 0.06 + 0.01) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']
result = pd.DataFrame({'REVENUE': [filtered_df['REVENUE'].sum()]})

# Output the result to a CSV file
result.to_csv('query_output.csv', index=False)
