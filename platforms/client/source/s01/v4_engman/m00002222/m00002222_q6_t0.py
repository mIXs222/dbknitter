import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis 
r = DirectRedis(host='redis', port=6379, db=0)

# Retrieve lineitem data from Redis
lineitem_data = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Filter the DataFrame
filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] > '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
    (lineitem_df['L_DISCOUNT'] >= 0.05) &
    (lineitem_df['L_DISCOUNT'] <= 0.07) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the revenue
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']

# Output the result to a CSV file
output = filtered_df[['REVENUE']].sum().reset_index()
output.columns = ['SUM_REVENUE']
output.to_csv('query_output.csv', index=False)
