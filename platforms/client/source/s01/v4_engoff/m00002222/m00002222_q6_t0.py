import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
redis_hostname = 'redis'
redis_port = 6379
r = DirectRedis(host=redis_hostname, port=redis_port, db=0)

# Fetch lineitem table from Redis
lineitem_data = r.get('lineitem')

# Deserialize from JSON to Pandas DataFrame
lineitem_df = pd.read_json(lineitem_data)

# Filter the DataFrame according to the query conditions
filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= pd.Timestamp('1994-01-01')) &
    (lineitem_df['L_SHIPDATE'] < pd.Timestamp('1995-01-01')) &
    (lineitem_df['L_DISCOUNT'] >= 0.06 - 0.01) &
    (lineitem_df['L_DISCOUNT'] <= 0.06 + 0.01) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate potential revenue increase
filtered_df['POTENTIAL_REVENUE_INCREASE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']

# Group by to simulate the sum operation and get the total
total_revenue_increase = filtered_df['POTENTIAL_REVENUE_INCREASE'].sum()

# Create output DataFrame
output_df = pd.DataFrame({'TOTAL_REVENUE_INCREASE': [total_revenue_increase]})

# Write output to CSV
output_df.to_csv('query_output.csv', index=False)
