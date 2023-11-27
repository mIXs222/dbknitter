import pandas as pd
import direct_redis

# Establish connection to the Redis server
hostname = 'redis'
port = 6379
database = 0

# Using direct_redis to interface as it is required
redis_client = direct_redis.DirectRedis(host=hostname, port=port, db=database)

# Load the lineitem table from Redis into a DataFrame
lineitem_key = 'lineitem'
lineitem_df = pd.read_json(redis_client.get(lineitem_key))

# Convert the SHIPDATE to datetime format for filtering
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter the data according to the specified conditions
filtered_lineitem = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
    (lineitem_df['L_DISCOUNT'] >= 0.06 - 0.01) &
    (lineitem_df['L_DISCOUNT'] <= 0.06 + 0.01) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the increased revenue if the discount had been eliminated
filtered_lineitem['revenue_increase'] = filtered_lineitem['L_EXTENDEDPRICE'] * filtered_lineitem['L_DISCOUNT']

# Calculate the sum of revenue_increase
total_revenue_increase = filtered_lineitem['revenue_increase'].sum()

# Convert to DataFrame to match expected output format
output_df = pd.DataFrame([{'Total Revenue Increase': total_revenue_increase}])

# Write output to CSV
output_df.to_csv('query_output.csv', index=False)
