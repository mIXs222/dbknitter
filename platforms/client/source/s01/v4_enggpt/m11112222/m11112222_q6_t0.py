import pandas as pd
import direct_redis

# Establish connection to the Redis database
redis_hostname = "redis"
redis_port = 6379
redis_db = 0
client = direct_redis.DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Load the lineitem table from Redis
lineitem_df = pd.read_json(client.get('lineitem'))

# Convert columns to appropriate data types
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_DISCOUNT'] = lineitem_df['L_DISCOUNT'].astype(float)
lineitem_df['L_QUANTITY'] = lineitem_df['L_QUANTITY'].astype(int)
lineitem_df['L_EXTENDEDPRICE'] = lineitem_df['L_EXTENDEDPRICE'].astype(float)

# Define the date range
start_date = pd.Timestamp('1994-01-01')
end_date = pd.Timestamp('1994-12-31')

# Perform the query to filter the data
filtered_lineitems = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) &
    (lineitem_df['L_SHIPDATE'] <= end_date) &
    (lineitem_df['L_DISCOUNT'] >= 0.06 - 0.01) &
    (lineitem_df['L_DISCOUNT'] <= 0.06 + 0.01) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the total revenue
filtered_lineitems['REVENUE'] = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])
total_revenue = filtered_lineitems['REVENUE'].sum()

# Write the output to a CSV file
output_df = pd.DataFrame({'Total_Revenue': [total_revenue]})
output_df.to_csv('query_output.csv', index=False)
