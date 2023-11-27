import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to the Redis database
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'orders' and 'lineitem' tables from Redis as Pandas DataFrames
orders_df = pd.read_json(redis_client.get('orders'))
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter lineitems on conditions related to ship mode, ship date, and commit date
filtered_lineitem = lineitem_df[
    ((lineitem_df['L_SHIPMODE'] == 'MAIL') | (lineitem_df['L_SHIPMODE'] == 'SHIP')) &
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_RECEIPTDATE'] >= '1994-01-01') &
    (lineitem_df['L_RECEIPTDATE'] < '1995-01-01')]

# Join 'orders' and 'filtered_lineitem' on 'O_ORDERKEY' and 'L_ORDERKEY'
merged_df = pd.merge(
    orders_df,
    filtered_lineitem,
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY',
    how='inner'
)

# Identify late lineitems and classify them based on priority and ship mode
late_lineitems = merged_df[merged_df['L_RECEIPTDATE'] > merged_df['L_COMMITDATE']]
late_lineitems['PRIORITY_GROUP'] = late_lineitems['O_ORDERPRIORITY'].apply(
    lambda x: 'HIGH-URGENT' if x in ['URGENT', 'HIGH'] else 'OTHER')

# Aggregate the count of late lineitems by PRIORITY_GROUP and L_SHIPMODE
result = late_lineitems.groupby(['PRIORITY_GROUP', 'L_SHIPMODE']).size().reset_index(name='LATE_COUNT')

# Write result to 'query_output.csv'
result.to_csv('query_output.csv', index=False)
