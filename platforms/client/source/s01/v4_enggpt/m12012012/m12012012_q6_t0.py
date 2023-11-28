import pandas as pd
from direct_redis import DirectRedis
import datetime

# Connect to Redis
redis_client = DirectRedis(
    host='redis',
    port=6379,
    db=0
)

# Fetching data from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Define the date range
start_date = datetime.datetime(1994, 1, 1)
end_date = datetime.datetime(1994, 12, 31)

# Filtering the DataFrame
filtered_lineitems = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date.strftime('%Y-%m-%d')) &
    (lineitem_df['L_SHIPDATE'] <= end_date.strftime('%Y-%m-%d')) &
    (lineitem_df['L_DISCOUNT'] >= 0.06 - 0.01) &
    (lineitem_df['L_DISCOUNT'] <= 0.06 + 0.01) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Compute total revenue
filtered_lineitems['REVENUE'] = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])
total_revenue = filtered_lineitems['REVENUE'].sum()

# Write output to a CSV file
pd.DataFrame({'Total_Revenue': [total_revenue]}).to_csv('query_output.csv', index=False)
