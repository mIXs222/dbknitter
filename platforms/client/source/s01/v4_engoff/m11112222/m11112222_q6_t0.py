# forecast_revenue_change.py
import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
hostname = 'redis'
port = 6379
database = 0
redis_client = DirectRedis(host=hostname, port=port, db=database)

# Retrieve lineitem data from Redis
lineitem_data = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Filter data as per the user's request
filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= pd.Timestamp('1994-01-01')) &
    (lineitem_df['L_SHIPDATE'] < pd.Timestamp('1995-01-01')) &
    (lineitem_df['L_DISCOUNT'] >= 0.01) & (lineitem_df['L_DISCOUNT'] <= 0.07) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the potential revenue increase
filtered_df['REVENUE_INCREASE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']
total_revenue_increase = filtered_df['REVENUE_INCREASE'].sum()

# Write the result to CSV
output_df = pd.DataFrame([{'TotalRevenueIncrease': total_revenue_increase}])
output_df.to_csv('query_output.csv', index=False)
