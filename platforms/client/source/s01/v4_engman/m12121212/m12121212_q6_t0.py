import pandas as pd
from direct_redis import DirectRedis

# Connect to Redis
redis_hostname = 'redis'
redis_port = 6379
redis_db = '0'
client = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)

# Fetch lineitem from Redis and load into DataFrame
lineitem_data_raw = client.get('lineitem')
lineitem_df = pd.read_json(lineitem_data_raw)

# Filter the DataFrame based on the conditions from the SQL-like query
filtered_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] > '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
    (lineitem_df['L_DISCOUNT'] >= (0.06 - 0.01)) &
    (lineitem_df['L_DISCOUNT'] <= (0.06 + 0.01)) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the revenue column
filtered_df['REVENUE'] = filtered_df['L_EXTENDEDPRICE'] * filtered_df['L_DISCOUNT']

# Sum up the revenue
total_revenue = filtered_df['REVENUE'].sum()

# Write to file
output_df = pd.DataFrame([{"REVENUE": total_revenue}])
output_df.to_csv('query_output.csv', index=False)
