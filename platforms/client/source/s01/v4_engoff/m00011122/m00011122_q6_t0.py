# forecasting_revenue_change.py
import pandas as pd
from direct_redis import DirectRedis

# Connection details
hostname = 'redis'
port = 6379
db_name = 0

# Establish Redis connection
redis_conn = DirectRedis(host=hostname, port=port, db=db_name)

# Retrieve the lineitem table from Redis as a DataFrame
lineitem_df = pd.read_json(redis_conn.get('lineitem'))

# Convert columns to appropriate types
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_DISCOUNT'] = lineitem_df['L_DISCOUNT'].astype(float)
lineitem_df['L_QUANTITY'] = lineitem_df['L_QUANTITY'].astype(float)
lineitem_df['L_EXTENDEDPRICE'] = lineitem_df['L_EXTENDEDPRICE'].astype(float)

# Define the date range for shipped items
start_date = pd.to_datetime('1994-01-01')
end_date = pd.to_datetime('1995-01-01')

# Calculate the revenue change
revenue_change_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) & 
    (lineitem_df['L_SHIPDATE'] < end_date) & 
    (lineitem_df['L_DISCOUNT'] >= 0.06 - 0.01) & 
    (lineitem_df['L_DISCOUNT'] <= 0.06 + 0.01) & 
    (lineitem_df['L_QUANTITY'] < 24)
].assign(revenue_increase=lambda x: x['L_EXTENDEDPRICE'] * x['L_DISCOUNT'])

# Sum revenue increase
total_revenue_increase = revenue_change_df['revenue_increase'].sum()

# Write the result to CSV
output = pd.DataFrame({'Total Revenue Increase': [total_revenue_increase]})
output.to_csv('query_output.csv', index=False)
