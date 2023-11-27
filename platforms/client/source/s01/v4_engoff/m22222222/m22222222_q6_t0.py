# forecast_revenue_change.py
import pandas as pd
from direct_redis import DirectRedis

# Connection information
database_name = '0'
port = 6379
hostname = 'redis'

# Initialize DirectRedis connection
redis_client = DirectRedis(host=hostname, port=port, db=database_name)

# Retrieve 'lineitem' table from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Filter rows within the desired ship date, discount, and quantity
filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= pd.Timestamp('1994-01-01')) &
    (lineitem_df['L_SHIPDATE'] < pd.Timestamp('1995-01-01')) &
    (lineitem_df['L_DISCOUNT'] >= (0.06 - 0.01)) &
    (lineitem_df['L_DISCOUNT'] <= (0.06 + 0.01)) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the potential revenue increase
filtered_df['POTENTIAL_REVENUE_INCREASE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']

# Calculate the total amount by which revenue would have increased
total_potential_increase = filtered_df['POTENTIAL_REVENUE_INCREASE'].sum()

# Write to csv file
output_df = pd.DataFrame({'TOTAL_REVENUE_INCREASE': [total_potential_increase]})
output_df.to_csv('query_output.csv', index=False)
