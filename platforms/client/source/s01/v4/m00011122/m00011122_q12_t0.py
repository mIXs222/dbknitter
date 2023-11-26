import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve data from Redis
orders_raw = redis_client.get('orders')
lineitem_raw = redis_client.get('lineitem')

# Convert data from Redis into Pandas DataFrame
orders = pd.read_json(orders_raw)
lineitem = pd.read_json(lineitem_raw)

# Merge the tables on order key
merged_data = pd.merge(orders, lineitem, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Apply conditions to the merged data
filtered_data = merged_data[
    (merged_data['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (merged_data['L_COMMITDATE'] < merged_data['L_RECEIPTDATE']) &
    (merged_data['L_SHIPDATE'] < merged_data['L_COMMITDATE']) &
    (merged_data['L_RECEIPTDATE'] >= '1994-01-01') &
    (filtered_data['L_RECEIPTDATE'] < '1995-01-01')
]

# Group by L_SHIPMODE with calculated counts
result = filtered_data.groupby('L_SHIPMODE').agg(
    HIGH_LINE_COUNT=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: (x.isin(['1-URGENT', '2-HIGH'])).sum()),
    LOW_LINE_COUNT=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: (~x.isin(['1-URGENT', '2-HIGH'])).sum())
).reset_index()

# Sort the result by L_SHIPMODE
result = result.sort_values(by='L_SHIPMODE')

# Write the output to a CSV file
result.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
