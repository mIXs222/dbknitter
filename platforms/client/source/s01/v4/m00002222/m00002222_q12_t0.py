import pandas as pd
from direct_redis import DirectRedis

# Initialize DirectRedis object
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Read tables from Redis
orders = pd.read_json(redis_client.get('orders'))
lineitem = pd.read_json(redis_client.get('lineitem'))

# Convert 'O_ORDERDATE' and 'L_COMMITDATE', 'L_RECEIPTDATE' and 'L_SHIPDATE' to datetime
orders['O_ORDERDATE'] = pd.to_datetime(orders['O_ORDERDATE'])
lineitem[['L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPDATE']] = lineitem[['L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPDATE']].apply(pd.to_datetime)

# Perform the SQL-like join and operations
merged = orders.merge(lineitem, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
filtered = merged[(merged['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
                  (merged['L_COMMITDATE'] < merged['L_RECEIPTDATE']) &
                  (merged['L_SHIPDATE'] < merged['L_COMMITDATE']) &
                  (merged['L_RECEIPTDATE'] >= '1994-01-01') &
                  (merged['L_RECEIPTDATE'] < '1995-01-01')]

result = filtered.groupby('L_SHIPMODE').agg(
    HIGH_LINE_COUNT=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: ((x == '1-URGENT') | (x == '2-HIGH')).sum()),
    LOW_LINE_COUNT=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: (~(x == '1-URGENT') & ~(x == '2-HIGH')).sum())
).reset_index()

# Order the results
result = result.sort_values('L_SHIPMODE')

# Write results to CSV
result.to_csv('query_output.csv', index=False)
