# query.py

import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
redis_host = "redis"
redis_port = 6379
redis_db = 0
redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db)

# Load data frame from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Filter based on criteria
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') & 
    (lineitem_df['L_SHIPDATE'] <= '1994-12-31') & 
    (lineitem_df['L_DISCOUNT'] >= 0.05) & 
    (lineitem_df['L_DISCOUNT'] <= 0.07) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate total revenue
filtered_lineitem_df['REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])
total_revenue = filtered_lineitem_df['REVENUE'].sum()

# Save the result to a CSV file
total_revenue_df = pd.DataFrame({'Total_Revenue': [total_revenue]})
total_revenue_df.to_csv('query_output.csv', index=False)
