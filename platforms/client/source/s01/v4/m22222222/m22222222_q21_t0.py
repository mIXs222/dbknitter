# query.py

import pandas as pd
import direct_redis

# Establish a connection to the Redis database
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Function to read data from Redis and convert it to a Pandas DataFrame
def read_redis(table_name):
    data = redis_client.get(table_name)
    return pd.read_json(data, orient='records')

# Read the relevant tables from Redis
nation_df = read_redis('nation')
supplier_df = read_redis('supplier')
orders_df = read_redis('orders')
lineitem_df = read_redis('lineitem')

# Start the processing of the SQL-like query in Pandas

# Filter for the given conditions
filtered_orders = orders_df[orders_df['O_ORDERSTATUS'] == 'F']
filtered_lineitem = lineitem_df[lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']]

# Existence conditions
exists_condition = lineitem_df.groupby('L_ORDERKEY').apply(
    lambda x: x['L_SUPPKEY'].nunique() > 1
).rename('condition').reset_index()
not_exists_condition = filtered_lineitem.groupby('L_ORDERKEY').apply(
    lambda x: x['L_SUPPKEY'].nunique() == 1
).rename('condition').reset_index()

# Merge the dataframes to filter the results
merge_df = supplier_df.merge(filtered_lineitem, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
merge_df = merge_df.merge(filtered_orders, left_on='L_ORDERKEY', right_on='O_ORDERKEY')
merge_df = merge_df.merge(nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA'], left_on='S_NATIONKEY', right_on='N_NATIONKEY')
merge_df = merge_df.merge(exists_condition, left_on='L_ORDERKEY', right_on='L_ORDERKEY')
merge_df = merge_df[merge_df['condition'] == True]
merge_df = merge_df.merge(not_exists_condition, left_on='L_ORDERKEY', right_on='L_ORDERKEY')
merge_df = merge_df[merge_df['condition'] == True]

# Group by S_NAME and count
result_df = merge_df.groupby('S_NAME').size().reset_index(name='NUMWAIT')

# Order by NUMWAIT DESC, S_NAME
result_df = result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
