import pandas as pd
from direct_redis import DirectRedis

def convert_shipdate(shipdate):
    return pd.to_datetime(shipdate)

# Connect to Redis and get the DataFrame
redis_hostname = 'redis'
redis_port = 6379
redis_db = 0
r = DirectRedis(host=redis_hostname, port=redis_port, db=redis_db)
lineitem_data = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Convert shipdate to datetime
lineitem_df['L_SHIPDATE'] = lineitem_df['L_SHIPDATE'].apply(convert_shipdate)

# Apply the query conditions
filtered_lineitem = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
    (lineitem_df['L_DISCOUNT'] >= .05) & (lineitem_df['L_DISCOUNT'] <= .07) &
    (lineitem_df['L_QUANTITY'] < 24)
]

# Calculate the SUM of L_EXTENDEDPRICE * L_DISCOUNT
revenue = filtered_lineitem.eval('L_EXTENDEDPRICE * L_DISCOUNT').sum()

# Write the output to query_output.csv
output_df = pd.DataFrame({'REVENUE': [revenue]})
output_df.to_csv('query_output.csv', index=False)
